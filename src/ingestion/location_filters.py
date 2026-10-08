"""
Classification utilities for OpenAQ location metadata already fetched
by fetch_locations.py (config/locations.json).

Consumers:
  - backfill ingestion: uses load_locations() for the full set,
    active or not — historical data doesn't expire.
  - daily Airflow DAG: uses classify_locations() and keeps only the
    active set, to avoid wasted daily S3 calls against stations that
    will never report again (see Phase 3 test: 297/566 locations
    returned skipped_no_source for a single day).
"""

import json
from datetime import datetime, timezone, timedelta

DEFAULT_LOCATIONS_PATH = "config/locations_metadata.json"
DEFAULT_ACTIVE_WINDOW_DAYS = 60


def load_locations(path: str = DEFAULT_LOCATIONS_PATH) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def classify_locations(
    locations: list[dict],
    active_window_days: int = DEFAULT_ACTIVE_WINDOW_DAYS,
) -> tuple[list[dict], list[dict]]:
    """
    Splits locations into (active, inactive) based on datetime_last_utc.
    'Active' = reported within the last `active_window_days` days.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=active_window_days)
    active, inactive = [], []

    for loc in locations:
        last = loc.get("datetime_last_utc")
        if last and datetime.fromisoformat(last.replace("Z", "+00:00")) > cutoff:
            active.append(loc)
        else:
            inactive.append(loc)

    return active, inactive


if __name__ == "__main__":
    locations = load_locations()
    active, inactive = classify_locations(locations)
    print(f"Active (reported in last {DEFAULT_ACTIVE_WINDOW_DAYS} days): {len(active)}")
    print(f"Inactive: {len(inactive)}")