from pyspark.sql import SparkSession
import platform
import socket


def main():
    # Create Spark session
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

    # Print basic environment info
    print("=== Environment Info ===")
    print(f"Hostname: {socket.gethostname()}")
    print(f"Python version: {platform.python_version()}")
    print(f"Spark version: {spark.version}")
    print("========================")

    # Create a simple RDD and transform it
    data = ["hello", "world", "from", "spark"]
    rdd = spark.sparkContext.parallelize(data)
    upper_rdd = rdd.map(lambda x: x.upper())

    # Collect and print
    result = upper_rdd.collect()
    print("=== Result ===")
    for word in result:
        print(word)
    print("==============")

    spark.stop()


if __name__ == "__main__":
    main()
