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
    finally:
        spark.stop()

    print("Q4 WORDCOUNT RESULT")
    print("Application ID: {}".format(application_id))
    print("Master: {}".format(master))
    print("Input: {}".format(args.input))
    print("{:<24} {:>5}".format("WORD", "COUNT"))
    for word, count in top_twenty:
        print("{:<24} {:>5}".format(word, count))
    print("RESULT_ROWS={}".format(len(top_twenty)))


if __name__ == "__main__":
    main()
