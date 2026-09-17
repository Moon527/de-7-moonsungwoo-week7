import argparse
import os

from pyspark.sql import SparkSession, functions as F, types as T


def analyze(input_path):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = SparkSession.builder.appName("Q2-analyze-moonsungwoo").config("spark.driver.host", "127.0.0.1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        schema = T.StructType([T.StructField("name", T.StringType()), T.StructField("age", T.IntegerType())])
        frame = spark.read.schema(schema).option("header", True).option("mode", "FAILFAST").csv(input_path)
        result = frame.agg(F.avg("age").alias("average_age"), F.max("age").alias("max_age"), F.count("*").alias("row_count"))
        values = result.first().asDict()
        print("Q2 CSV INPUT: " + input_path, flush=True)
        result.show(truncate=False)
        print("Q2 CSV RESULT: average_age={average_age} max_age={max_age} row_count={row_count}".format(**values), flush=True)
        return values
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="/opt/airflow/data/data.csv")
    analyze(parser.parse_args().input)
