from pyspark.sql import SparkSession
import platform
import socket
from google.cloud import logging as cloud_logging
import logging


client = cloud_logging.Client()
client.setup_logging()

logger = logging.getLogger("HelloWorld")
logger.setLevel(logging.INFO)


def main():
    # Create Spark session
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

    print("hello")

    # Log basic environment info
    logger.info("=== Environment Info ===")
    logger.info(f"Hostname: {socket.gethostname()}")
    logger.info(f"Python version: {platform.python_version()}")
    logger.info(f"Spark version: {spark.version}")
    logger.info("========================")

    # Create a simple RDD and transform it
    data = ["hello", "world", "from", "spark"]
    rdd = spark.sparkContext.parallelize(data)
    upper_rdd = rdd.map(lambda x: x.upper())

    # Collect and log
    result = upper_rdd.collect()
    logger.info("=== Result ===")
    for word in result:
        logger.info(word)

    spark.stop()


main()
