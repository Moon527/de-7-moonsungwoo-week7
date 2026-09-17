import argparse
import os

from pyspark.sql import SparkSession, functions as F


def process_stream(host, port, seconds):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = SparkSession.builder.appName("Q2-stream-moonsungwoo").config("spark.driver.host", "127.0.0.1").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        lines = spark.readStream.format("socket").option("host", host).option("port", port).load()
        counts = lines.filter(F.col("value").contains("ERROR")).groupBy().count()
        query = counts.writeStream.format("console").outputMode("complete").option("truncate", False).trigger(processingTime="1 second").start()
        print("Q2 STREAM: socket={}:{} filter=ERROR sink=console".format(host, port), flush=True)
        query.awaitTermination(seconds)
        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))
    finally:
        if query is not None:
            query.stop()
        spark.stop()
    print("Q2 STREAM FINISHED", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    if args.seconds < 1:
        parser.error("--seconds must be positive")
    process_stream(args.host, args.port, args.seconds)
