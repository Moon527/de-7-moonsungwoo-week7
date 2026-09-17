import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def aggregate_titles(frame, min_year):
    return (
        frame.filter(F.col("release_year").cast("int") >= F.lit(min_year))
        .select("type", F.explode(F.split(F.col("listed_in"), ",")).alias("genre"))
        .withColumn("genre", F.trim(F.col("genre")))
        .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
        .groupBy("type", "genre")
        .agg(F.count(F.lit(1)).alias("count"))
    )


def transform(input_path, output_path, summary_path, min_year):
    spark = SparkSession.builder.appName("Q9-netflix-moonsungwoo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        frame = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .option("mode", "FAILFAST")
            .csv(str(input_path))
        )
        required = {"type", "release_year", "listed_in"}
        if not required.issubset(frame.columns):
            raise ValueError("Missing required Netflix CSV columns")
        input_rows = frame.count()
        result = aggregate_titles(frame, min_year).cache()
        aggregate_rows = result.count()
        if aggregate_rows == 0:
            raise ValueError("No aggregate rows; check min_year and input data")
        result.orderBy("type", "genre").coalesce(1).write.mode("overwrite").option(
            "compression", "snappy"
        ).parquet(str(output_path))
        saved_rows = spark.read.parquet(str(output_path)).count()
        if saved_rows != aggregate_rows:
            raise ValueError("Parquet row count does not match the aggregation")
        summary = {
            "input_rows": input_rows,
            "aggregate_rows": aggregate_rows,
            "min_year": min_year,
            "output_path": str(output_path),
        }
        Path(summary_path).write_text(json.dumps(summary), encoding="utf-8")
        print("Q9 TRANSFORM: " + json.dumps(summary), flush=True)
        return summary
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Aggregate Netflix titles by type and genre")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--min-year", type=int, default=2015)
    args = parser.parse_args()
    transform(args.input, args.output, args.summary, args.min_year)


if __name__ == "__main__":
    main()
