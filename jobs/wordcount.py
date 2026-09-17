import argparse
from operator import add

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser(description="Q4 whitespace-only word count")
    parser.add_argument("--input", default="/opt/spark/data/wordcount.txt")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Q4-wordcount-moonsungwoo").getOrCreate()
    try:
        context = spark.sparkContext
        context.setLogLevel("WARN")
        master = context.master
        application_id = context.applicationId
        if not master.startswith("spark://"):
            raise ValueError("Q4 requires submission to a Spark Standalone cluster")

        counts = (
            context.textFile(args.input)
            .flatMap(lambda line: line.split())
            .map(lambda word: (word, 1))
            .reduceByKey(add)
        )
        top_twenty = counts.takeOrdered(20, key=lambda item: (-item[1], item[0]))
        result = spark.createDataFrame(top_twenty, "word STRING, count LONG")
        print("Q4 WORDCOUNT RESULT", flush=True)
        print("Application ID: {}".format(application_id), flush=True)
        print("Master: {}".format(master), flush=True)
        print("Input: {}".format(args.input), flush=True)
        result.orderBy(result["count"].desc(), result["word"].asc()).show(20, truncate=False)
        print("RESULT_ROWS={}".format(len(top_twenty)), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
