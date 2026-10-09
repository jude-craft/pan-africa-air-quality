"""
Phase 3: Bronze Ingestion — Daily.

Defaults to yesterday (UTC) when no date is given, since a station's
current day isn't fully reported partway through. Only processes
locations classified as active (see location_filters.py) — running
against all 566 locations daily, forever, wastes calls on the ~181
that will never report again (confirmed in testing: 297/566 returned
skipped_no_source for a single day).
"""

import os
import sys
import time
import argparse
import boto3
from datetime import datetime, timezone, timedelta
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from src.ingestion.location_filters import load_locations, classify_locations

OPENAQ_SOURCE_BUCKET = "openaq-data-archive"


def build_source_key(location_id: int, date_str: str) -> str:
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


def ingest_date(date_str: str, locations: list[dict], bronze_bucket: str) -> dict:
    s3_client = boto3.client(
        "s3", config=Config(retries={"max_attempts": 5, "mode": "adaptive"})
    )
    summary = {"copied": 0, "skipped_exists": 0, "skipped_no_source": 0}

    for loc in locations:
        status = copy_location_day(
            s3_client, bronze_bucket, loc["location_id"], loc["country_code"], date_str
        )
        summary[status] += 1

    return summary


def default_ingestion_date() -> str:
    """Yesterday, UTC — today's data isn't fully reported yet."""
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "date", nargs="?", default=None,
        help="YYYY-MM-DD. Defaults to yesterday (UTC) if omitted — the normal daily-run case."
    )
    args = parser.parse_args()

    date_str = args.date or default_ingestion_date()
    bronze_bucket = os.environ["BRONZE_BUCKET"]

    all_locations = load_locations()
    active_locations, inactive_locations = classify_locations(all_locations)

    print(f"Ingesting {date_str} | {len(active_locations)} active locations "
          f"({len(inactive_locations)} inactive, skipped)")

    result = ingest_date(date_str, active_locations, bronze_bucket)
    print(f"Bronze ingestion for {date_str}: {result}")