import argparse
import logging
import re
import fnmatch
from datetime import datetime

from pyspark.sql import SparkSession
from google.cloud import logging as cloud_logging
from google.cloud import storage
from pyspark.sql.functions import lit, input_file_name, udf
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


def expand_gcs_paths(path_pattern, logger=None):
    """
    Expand GCS wildcard patterns to list of matching folder paths.

    Args:
        path_pattern: GCS path with optional wildcards (e.g., gs://bucket/data/202512*)
        logger: Optional logger for debugging

    Returns:
        List of matching GCS folder paths
    """
    # If no wildcard, return as-is
    if '*' not in path_pattern and '?' not in path_pattern:
        return [path_pattern]

    if not path_pattern.startswith('gs://'):
        raise ValueError(f"Only GCS paths (gs://) are supported for wildcards: {path_pattern}")

    # Parse GCS path: gs://bucket/path/pattern*
    path_without_scheme = path_pattern[5:]  # Remove 'gs://'
    parts = path_without_scheme.split('/')
    bucket_name = parts[0]
    path_parts = parts[1:]

    # Find where the wildcard starts
    wildcard_pattern = '/'.join(path_parts)

    # Get prefix before first wildcard for efficient listing
    prefix_before_wildcard = ''
    for i, part in enumerate(path_parts):
        if '*' in part or '?' in part:
            prefix_before_wildcard = '/'.join(path_parts[:i])
            break

    if logger:
        logger.debug(
            "Expanding GCS path",
            extra={
                "json_fields": {
                    "bucket": bucket_name,
                    "prefix": prefix_before_wildcard,
                    "pattern": wildcard_pattern,
                }
            },
        )

    # List all prefixes (folders) under the prefix
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Use delimiter='/' to get folder-like structure
    # Add trailing slash to prefix if not empty
    list_prefix = prefix_before_wildcard + '/' if prefix_before_wildcard else ''
    blobs_iterator = bucket.list_blobs(prefix=list_prefix, delimiter='/')

    # IMPORTANT: Must consume the iterator to populate the prefixes property
    _ = list(blobs_iterator)

    matching_paths = set()

    # Check prefixes (folders)
    if logger:
        logger.debug(
            "Checking prefixes for wildcard match",
            extra={
                "json_fields": {
                    "wildcard_pattern": wildcard_pattern,
                    "prefix_before_wildcard": prefix_before_wildcard,
                    "list_prefix": list_prefix,
                    "prefixes_found": len(list(blobs_iterator.prefixes)),
                }
            },
        )

    for prefix in blobs_iterator.prefixes:
        # Remove trailing slash
        folder_path = prefix.rstrip('/')
        matches = fnmatch.fnmatch(folder_path, wildcard_pattern)

        if logger:
            logger.debug(
                "Checking prefix",
                extra={
                    "json_fields": {
                        "prefix": prefix,
                        "folder_path": folder_path,
                        "wildcard_pattern": wildcard_pattern,
                        "matches": matches,
                    }
                },
            )

        if matches:
            full_path = f'gs://{bucket_name}/{folder_path}'
            matching_paths.add(full_path)

    result = sorted(matching_paths)

    if logger:
        logger.info(
            "Expanded GCS paths",
            extra={
                "json_fields": {
                    "pattern": path_pattern,
                    "matched_count": len(result),
                    "matched_paths": result,
                }
            },
        )

    return result


def parse_args():
    parser = argparse.ArgumentParser(
        description="Overwrite multiple partitions in BigQuery from GCS data using wildcards."
    )

    parser.add_argument(
        "--run_uuid", required=True, help="Unique identifier for the run."
    )

    parser.add_argument(
        "--input_path",
        required=True,
        help="GCS path to input data with optional wildcards (e.g., gs://bucket/data/202512*).",
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
    logger = logging.getLogger("storageflow.overwrite_partition_range")
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

    spark = SparkSession.builder.appName("OverwritePartitionRangeToBQ").getOrCreate()

    # Expand wildcard paths
    try:
        expanded_paths = expand_gcs_paths(args.input_path, logger)

        if not expanded_paths:
            raise ValueError(f"No paths matched pattern: {args.input_path}")

        logger.info(
            "Loading data from expanded paths",
            extra={
                "json_fields": {
                    "run_uuid": str(uuid),
                    "pattern": args.input_path,
                    "path_count": len(expanded_paths),
                    "paths": expanded_paths,
                }
            },
        )

        # Load all data from the expanded paths
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
                df = reader.schema(schema).format(args.format).load(expanded_paths)
            else:
                df = (
                    reader.option("inferSchema", True)
                    .format(args.format)
                    .load(expanded_paths)
                )
        else:
            if args.compression:
                reader = reader.option("compression", args.compression)
            df = reader.format(args.format).load(expanded_paths)

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

        # Use appropriate return type based on partition_column_type
        return_type = (
            DateType() if args.partition_column_type == "date" else TimestampType()
        )
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

        # Get unique partition values from the DataFrame to delete before appending
        partition_values = [
            row[args.partition_column]
            for row in df.select(args.partition_column).distinct().collect()
        ]

        logger.info(
            "Partitions to be replaced",
            extra={
                "json_fields": {
                    "table": table_fqn,
                    "run_uuid": str(uuid),
                    "partition_count": len(partition_values),
                    "partitions": [str(v) for v in partition_values],
                }
            },
        )

        # Delete existing partitions using BigQuery API
        from google.cloud import bigquery

        bq_client = bigquery.Client()
        project_id = bq_client.project
        dataset_id, table_id = table_fqn.split(".")

        for partition_value in partition_values:
            # Format partition value based on type (YYYYMMDD for both date and timestamp)
            if isinstance(partition_value, datetime):
                partition_decorator = partition_value.strftime("%Y%m%d")
            else:
                # It's already a date object
                partition_decorator = partition_value.strftime("%Y%m%d")

            # Use BigQuery partition decorator to delete specific partition
            partition_table = f"{project_id}.{dataset_id}.{table_id}${partition_decorator}"

            logger.info(
                "Deleting partition",
                extra={
                    "json_fields": {
                        "table": partition_table,
                        "partition_value": str(partition_value),
                        "run_uuid": str(uuid),
                    }
                },
            )

            try:
                # Delete the partition table
                bq_client.delete_table(partition_table, not_found_ok=True)
                logger.info(
                    "Partition deleted",
                    extra={
                        "json_fields": {
                            "partition": partition_decorator,
                            "run_uuid": str(uuid),
                        }
                    },
                )
            except Exception as e:
                logger.warning(
                    "Failed to delete partition (may not exist)",
                    extra={
                        "json_fields": {
                            "partition": partition_decorator,
                            "error": str(e),
                            "run_uuid": str(uuid),
                        }
                    },
                )

        logger.info(
            "Preparing to append data to BigQuery",
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

        # Append the new data (partitions were already deleted above)
        writer.mode("append").save(table_fqn)

        logger.info(
            "Partitions overwritten",
            extra={
                "json_fields": {
                    "table": table_fqn,
                    "run_uuid": str(uuid),
                    "path_count": len(expanded_paths),
                }
            },
        )
    except Exception as e:
        logger.error(
            "Failed to overwrite partitions",
            exc_info=True,
            extra={
                "json_fields": {
                    "error": str(e),
                    "table": table_fqn if "table_fqn" in locals() else "unknown",
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
