import argparse
import logging
import re
import os
from datetime import datetime

from pyspark.sql import SparkSession
from google.cloud import logging as cloud_logging
from pyspark.sql.functions import lit, to_date, input_file_name
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
)
import hashlib
import uuid


def compute_uuid(name: str) -> uuid.UUID:
    digest = hashlib.md5(name.encode()).digest()
    return uuid.UUID(bytes=digest)


def parse_separator(sep_str: str) -> str:
    if not sep_str:
        raise ValueError("Separator string cannot be empty.")
    sep_str = sep_str.strip()
    try:
        if re.fullmatch(r"[uU]\+[0-9a-fA-F]{4,6}", sep_str):
            return chr(int(sep_str[2:], 16))
        elif sep_str.startswith(("0x", "0X")):
            return chr(int(sep_str, 16))
        elif sep_str.startswith(("0o", "0O")):
            return chr(int(sep_str, 8))
        elif sep_str.startswith(("0b", "0B")):
            return chr(int(sep_str, 2))
        elif len(sep_str) == 1:
            return sep_str
        else:
            raise ValueError(f"Unrecognized format: {sep_str}")
    except Exception as e:
        raise ValueError(f"Invalid separator '{sep_str}': {e}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Overwrite entire BigQuery table from GCS partitioned data."
    )

    parser.add_argument(
        "--run_uuid", required=True, help="Unique identifier for the run."
    )

    parser.add_argument(
        "--input_path", required=True, help="GCS path to partitioned data folder."
    )
    parser.add_argument(
        "--format",
        required=True,
        choices=["parquet", "orc", "avro", "json", "delimited"],
        help="Input file format.",
    )
    parser.add_argument("--dataset", required=True, help="BigQuery dataset name.")
    parser.add_argument("--table", required=True, help="BigQuery table name.")
    parser.add_argument(
        "--temp_bucket", required=True, help="GCS bucket for BigQuery staging."
    )
    parser.add_argument(
        "--delimiter", help="Delimiter for delimited files (eg CSV, TSV)"
    )
    parser.add_argument(
        "--has_header", action="store_true", help="Set if input has header."
    )
    parser.add_argument(
        "--compression",
        help="Compression codec (e.g. gzip, bzip2, snappy) if file lacks extension.",
    )
    parser.add_argument(
        "--schema", help="Comma-separated column:type for files w/o headers."
    )
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--partition_column", default="date", help="Partition column name."
    )
    parser.add_argument(
        "--partition_regex",
        required=True,
        help="Regex to extract partition value from file paths.",
    )
    parser.add_argument(
        "--partition_format",
        required=True,
        help="How to interpret extracted partition value. Values come from Python function strptime",
    )
    parser.add_argument(
        "--partition_type",
        required=True,
        choices=["HOUR", "DAY", "MONTH", "YEAR"],
        help="BigQuery partition column type.",
    )
    parser.add_argument(
        "--create_table",
        action="store_true",
        help="If set, include partition info for table creation.",
    )
    parser.add_argument(
        "--partition_column_type",
        default="timestamp",
        choices=["date", "timestamp"],
        help="Data type for the partition column (default: timestamp).",
    )
    parser.add_argument(
        "--clustering_fields",
        help="Comma-separated list of fields for BigQuery clustering.",
    )
    return parser.parse_args()


def configure_logging(level):
    cloud_client = cloud_logging.Client()
    cloud_client.setup_logging()
    logger = logging.getLogger("storageflow.overwrite_table")
    logger.setLevel(level)
    return logger


def extract_partition_from_path(path, pattern):
    match = re.search(pattern, path)
    if not match:
        raise ValueError(f"Could not extract partition value from path: {path}")
    return match.group(1)


def parse_schema(schema_str):
    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }
    fields = []
    for col in schema_str.split(","):
        name, dtype = col.strip().split(":")
        dtype_lower = dtype.strip().lower()
        if dtype_lower not in type_map:
            raise ValueError(f"Unsupported type: {dtype_lower}")
        fields.append(StructField(name.strip(), type_map[dtype_lower], True))
    return StructType(fields)


def cast_partition_value(value, type_, fmt):
    try:
        dt = datetime.strptime(value, fmt)
        return dt
    except ValueError as e:
        raise ValueError(
            f"Failed to parse partition value '{value}' with format '{fmt}' for partition_type '{type_}' : {e}"
        )


def main():
    args = parse_args()

    uuid = args.run_uuid

    logger = configure_logging(args.log_level)

    args_dict = vars(args)
    logger.info(
        "Starting script",
        extra={"json_fields": {"run_uuid": str(uuid), "args": args_dict}},
    )

    spark = SparkSession.builder.appName("OverwriteTableToBQ").getOrCreate()

    logger.info(
        "Loading data from partitioned folder",
        extra={"json_fields": {"run_uuid": str(uuid), "path": args.input_path}},
    )

    # Load all data from the partitioned folder
    reader = spark.read
    if args.format in ("delimited"):
        args.format = "csv"  # csv is the right type when reading delimited files
        if not args.delimiter:
            raise ValueError(
                "Delimiter must be specified for delimited formats (--delimiter)."
            )
        sep = parse_separator(args.delimiter)
        reader = reader.option("sep", sep)
        reader = reader.option("header", str(args.has_header).lower())
        if args.compression:
            reader = reader.option("compression", args.compression)
        if args.schema:
            schema = parse_schema(args.schema)
            df = reader.schema(schema).format(args.format).load(args.input_path)
        else:
            df = (
                reader.option("inferSchema", True)
                .format(args.format)
                .load(args.input_path)
            )
    else:
        if args.compression:
            reader = reader.option("compression", args.compression)
        df = reader.format(args.format).load(args.input_path)

    # Add input_file_name column to extract partition values from file paths
    df = df.withColumn("_input_file_name", input_file_name())

    if logger.isEnabledFor(logging.DEBUG):
        row_count = df.count()
    else:
        row_count = "not computed"

    logger.info(
        "Data loaded",
        extra={
            "json_fields": {
                "run_uuid": str(uuid),
                "row_count": row_count,
                "columns": df.columns,
            }
        },
    )

    table_fqn = f"{args.dataset}.{args.table}"

    # Extract partition values from file paths and add partition column
    try:

        def extract_and_cast_partition(file_path):
            extracted = extract_partition_from_path(file_path, args.partition_regex)
            parsed_value = cast_partition_value(
                extracted, args.partition_type, args.partition_format
            )

            # Convert to desired output type
            if args.partition_column_type == "date":
                return parsed_value.date()
            else:  # timestamp
                return parsed_value

        # Register UDF to extract partition from file path
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DateType

        # Use appropriate return type based on partition_column_type
        return_type = DateType() if args.partition_column_type == "date" else TimestampType()
        extract_partition_udf = udf(extract_and_cast_partition, return_type)

        # Add partition column based on file paths
        df = df.withColumn(
            args.partition_column, extract_partition_udf(df["_input_file_name"])
        )

        # Drop the temporary input file name column
        df = df.drop("_input_file_name")

        logger.info(
            "Partition column added to all rows",
            extra={
                "json_fields": {
                    "run_uuid": str(uuid),
                    "partition_column": args.partition_column,
                }
            },
        )

        logger.info(
            "Preparing to write entire table to BigQuery",
            extra={"json_fields": {"table": table_fqn, "run_uuid": str(uuid)}},
        )

        writer = df.write.format("bigquery")

        if not args.create_table:
            writer = writer.option("createDisposition", "CREATE_NEVER")

        writer = (
            writer.option("partitionField", args.partition_column)
            .option("partitionType", args.partition_type)
            .option("temporaryGcsBucket", args.temp_bucket)
            .option("allowFieldAddition", "true")
            .option("allowFieldRelaxation", "true")
        )

        if args.clustering_fields:
            writer = writer.option("clusteredFields", args.clustering_fields)

        # Overwrite entire table
        writer.mode("overwrite").save(table_fqn)

        logger.info(
            "Table overwritten",
            extra={
                "json_fields": {
                    "table": table_fqn,
                    "run_uuid": str(uuid),
                }
            },
        )
    except Exception as e:
        logger.error(
            "Failed to overwrite table",
            exc_info=True,
            extra={
                "json_fields": {
                    "error": str(e),
                    "table": table_fqn,
                    "run_uuid": str(uuid),
                }
            },
        )
        raise
    finally:
        spark.stop()
        logger.info(
            "Spark session stopped", extra={"json_fields": {"run_uuid": str(uuid)}}
        )


if __name__ == "__main__":
    main()

