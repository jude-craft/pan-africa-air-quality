"""
Phase 3: Bronze Ingestion DAG.

Daily DAG that copies each known OpenAQ location's gzip file for the
DAG's logical date ({{ ds }}) into the Bronze S3 bucket. Uses dynamic
task mapping so each location is an independently retryable task
instance, visible in the Airflow UI.
"""

import json
import os
from datetime import datetime

from airflow.decorators import dag, task

BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
LOCATIONS_METADATA_PATH = "/opt/airflow/config/locations_metadata.json"


@dag(
    dag_id="bronze_ingestion",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=10,
    tags=["bronze", "openaq"],
)
def bronze_ingestion_dag():

    @task
    def load_locations() -> list[dict]:
        with open(LOCATIONS_METADATA_PATH) as f:
            return json.load(f)

    @task
    def copy_location_for_date(location: dict, ds: str = None) -> str:
        import boto3
        from ingestion.openaq_to_bronze import copy_location_day

        s3_client = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        status = copy_location_day(
            s3_client, BRONZE_BUCKET, location["location_id"],
            location["country_code"], ds,
        )
        return f"{location['location_id']} ({location['country_code']}): {status}"

    locations = load_locations()
    copy_location_for_date.expand(location=locations)


bronze_ingestion_dag()