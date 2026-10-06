"""
Phase 3: Bronze Ingestion.

Server-side copies daily OpenAQ gzip files from the public
openaq-data-archive bucket into our own Bronze S3 bucket, partitioned
by country/location/date. Idempotent: skips files that already exist
at the destination, and treats a missing source file as a normal
"station didn't report that day" case, not an error.
"""

import os
import json
import boto3
from botocore.exceptions import ClientError

OPENAQ_SOURCE_BUCKET = "openaq-data-archive"


def build_source_key(location_id: int, date_str: str) -> str:
    """date_str format: YYYY-MM-DD"""
    year, month, day = date_str.split("-")
    return (
        f"records/csv.gz/locationid={location_id}/"
        f"year={year}/month={month}/"
        f"location-{location_id}-{year}{month}{day}.csv.gz"
    )


def build_destination_key(location_id: int, country_code: str, date_str: str) -> str:
    year, month, day = date_str.split("-")
    return (
        f"openaq/country={country_code}/locationid={location_id}/"
        f"year={year}/month={month}/day={day}/"
        f"location-{location_id}-{year}{month}{day}.csv.gz"
    )


def destination_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def copy_location_day(s3_client, bronze_bucket: str, location_id: int,
                       country_code: str, date_str: str) -> str:
    """
    Copies one location's data for one date from the public OpenAQ
    archive into our Bronze bucket. Returns a status string:
    'copied', 'skipped_exists', or 'skipped_no_source'.
    """
    source_key = build_source_key(location_id, date_str)
    dest_key = build_destination_key(location_id, country_code, date_str)

    if destination_exists(s3_client, bronze_bucket, dest_key):
        return "skipped_exists"

    try:
        s3_client.copy_object(
            Bucket=bronze_bucket,
            Key=dest_key,
            CopySource={"Bucket": OPENAQ_SOURCE_BUCKET, "Key": source_key},
        )
        return "copied"
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return "skipped_no_source"
        raise


def ingest_date(date_str: str, locations_metadata_path: str, bronze_bucket: str) -> dict:
    """
    Runs the Bronze copy for every known location, for a single date.
    Used for local testing — the DAG calls copy_location_day directly
    per mapped task instance instead.
    """
    with open(locations_metadata_path) as f:
        locations = json.load(f)

    s3_client = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    summary = {"copied": 0, "skipped_exists": 0, "skipped_no_source": 0}

    for loc in locations:
        status = copy_location_day(
            s3_client, bronze_bucket, loc["location_id"], loc["country_code"], date_str
        )
        summary[status] += 1

    return summary


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    

    load_dotenv()
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-10-01"
    bronze_bucket = os.environ["BRONZE_BUCKET"]
    result = ingest_date(date_str, "config/locations_metadata.json", bronze_bucket)
    print(f"Bronze ingestion for {date_str}: {result}")