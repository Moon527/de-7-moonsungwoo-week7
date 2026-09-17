import argparse
import csv
from pathlib import Path

import boto3


def download_dataset(s3, bucket, output_dir):
    key = "bronze/netflix_titles.csv"
    found = False
    paginator = s3.get_paginator("list_objects_v2")
    print("S3 objects: s3://{}/bronze/".format(bucket))
    for page in paginator.paginate(Bucket=bucket, Prefix="bronze/"):
        for item in page.get("Contents", []):
            print("{}\t{} bytes".format(item["Key"], item["Size"]))
            found = found or item["Key"] == key
    if not found:
        raise FileNotFoundError("Upload {} to bucket {} first".format(key, bucket))

    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / "netflix_titles.csv"
    temporary = output_dir / "netflix_titles.csv.part"
    try:
        s3.download_file(bucket, key, str(temporary))
        # CSV fields may contain quoted newlines; count parsed records, not lines.
        with temporary.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream)
            header = next(reader, None)
            required = {"type", "title", "release_year", "listed_in"}
            if not header or not required.issubset(header):
                raise ValueError("CSV does not contain the expected Netflix columns")
            row_count = sum(1 for row in reader if row)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    print("Downloaded: {}".format(destination.resolve()))
    print("CSV_COLUMNS={}".format(len(header)))
    print("CSV_DATA_ROWS={} (header excluded)".format(row_count))
    print("FILE_BYTES={}".format(destination.stat().st_size))
    return row_count


def main():
    parser = argparse.ArgumentParser(description="Download and count the Q8 Netflix CSV")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default="week7")
    parser.add_argument("--region", default="ap-northeast-2")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parent / "data")
    args = parser.parse_args()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    download_dataset(session.client("s3"), args.bucket, args.output_dir)


if __name__ == "__main__":
    main()
