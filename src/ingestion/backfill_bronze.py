"""
Backfill Ingestion (one-time, full history).
"""

import os
import sys
import time
import argparse
import boto3
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from src.ingestion.location_filters import load_locations
from src.ingestion.openaq_to_bronze import OPENAQ_SOURCE_BUCKET, build_destination_key

MAX_WORKERS = 10  # modest, given observed network instability this session


def make_s3_client() -> boto3.client:
    """Each thread needs its own client — boto3 clients aren't thread-safe to share."""
    return boto3.client(
        "s3",
        config=Config(retries={"max_attempts": 5, "mode": "adaptive"}),
    )


def list_source_keys(s3_client, location_id: int) -> list[str]:
    """The real inventory OpenAQ has published for this location — not a guess."""
    prefix = f"records/csv.gz/locationid={location_id}/"
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=OPENAQ_SOURCE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def list_already_copied_filenames(s3_client, bronze_bucket: str, location_id: int, country_code: str) -> set[str]:
    """What we already have in Bronze for this location — bulk listing, not per-file checks."""
    prefix = f"openaq/country={country_code}/locationid={location_id}/"
    filenames = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bronze_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            filenames.add(obj["Key"].split("/")[-1])
    return filenames


def extract_date_str(source_key: str) -> str:
    """location-{id}-{yyyymmdd}.csv.gz -> 'YYYY-MM-DD'"""
    filename = source_key.split("/")[-1]
    yyyymmdd = filename.split("-")[-1].replace(".csv.gz", "")
    return f"{yyyymmdd[0:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"


def backfill_location(s3_client, bronze_bucket: str, location: dict) -> dict:
    location_id = location["location_id"]
    country_code = location["country_code"]

    source_keys = list_source_keys(s3_client, location_id)
    already_copied = list_already_copied_filenames(s3_client, bronze_bucket, location_id, country_code)

    to_copy = [k for k in source_keys if k.split("/")[-1] not in already_copied]

    copied, failed = 0, 0
    for source_key in to_copy:
        date_str = extract_date_str(source_key)
        # Reuses the SAME destination-path logic as the daily job —
        # one source of truth for how Bronze keys are built.
        dest_key = build_destination_key(location_id, country_code, date_str)
        try:
            s3_client.copy_object(
                Bucket=bronze_bucket,
                Key=dest_key,
                CopySource={"Bucket": OPENAQ_SOURCE_BUCKET, "Key": source_key},
            )
            copied += 1
        except Exception as e:
            print(f"  FAILED: {source_key} -> {dest_key}: {e}")
            failed += 1

    return {
        "location_id": location_id,
        "name": location["name"],
        "country_code": country_code,
        "total_source_files": len(source_keys),
        "already_had": len(source_keys) - len(to_copy),
        "copied": copied,
        "failed": failed,
    }


def run_backfill(bronze_bucket: str, locations: list[dict], max_workers: int = MAX_WORKERS):
    total = len(locations)
    start_time = time.time()
    results = []

    def worker(location):
        return backfill_location(make_s3_client(), bronze_bucket, location)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, loc): loc for loc in locations}
        for i, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            results.append(result)
            elapsed = time.time() - start_time
            print(
                f"[{i}/{total}] {result['name']} ({result['country_code']}): "
                f"{result['copied']} copied, {result['already_had']} already had, "
                f"{result['failed']} failed | elapsed={elapsed:.0f}s",
                flush=True,
            )

    print("\n--- Backfill Summary ---")
    print(f"Total source files found: {sum(r['total_source_files'] for r in results)}")
    print(f"Newly copied: {sum(r['copied'] for r in results)}")
    print(f"Failed: {sum(r['failed'] for r in results)}")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", help="Limit to one country code, e.g. KE (for testing before a full run)")
    args = parser.parse_args()

    load_dotenv()
    bronze_bucket = os.environ["BRONZE_BUCKET"]

    locations = load_locations()
    if args.country:
        locations = [loc for loc in locations if loc["country_code"] == args.country]

    run_backfill(bronze_bucket, locations)