import argparse
import os

from pyspark.sql import SparkSession, functions as F, types as T


def age_group(age):
    if age is None:
        return None
    return "30_or_over" if age >= 30 else "under_30"


def apply_udf(input_path, output_path):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = SparkSession.builder.appName("Q2-udf-moonsungwoo").config("spark.driver.host", "127.0.0.1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        schema = T.StructType([T.StructField("name", T.StringType()), T.StructField("age", T.IntegerType())])
        frame = spark.read.schema(schema).option("header", True).option("mode", "FAILFAST").csv(input_path)
        group_udf = F.udf(age_group, T.StringType())
        result = frame.withColumn("age_group", group_udf(F.col("age")))
        result.orderBy("name").coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
        saved = spark.read.option("header", True).csv(output_path)
        print("Q2 UDF OUTPUT: " + output_path, flush=True)
        saved.orderBy("name").show(truncate=False)
        count = saved.count()
        print("Q2 UDF ROWS={}".format(count), flush=True)
        return count
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="/opt/airflow/data/data.csv")
    # Spark overwrite must target a child directory, not the bind-mount root.
    parser.add_argument("--output", default="/opt/airflow/q2/output/age_groups")
    args = parser.parse_args()
    apply_udf(args.input, args.output)
