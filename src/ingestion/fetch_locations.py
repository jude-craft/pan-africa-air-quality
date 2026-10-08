"""
Phase 2: Data Discovery.

Queries the OpenAQ v3 /locations endpoint for each target country and
writes a flat metadata file that Phase 3 (Bronze ingestion) reads as its
dynamic configuration — which stations exist, which provider they belong
to, and what date range of history is actually available.

This is a manual/periodic script, not an Airflow DAG: location metadata
doesn't change hour to hour the way measurements do.
"""

import os
import sys
import time
import json
import requests
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from config.target_countries import TARGET_COUNTRIES

OPENAQ_BASE_URL = "https://api.openaq.org/v3/locations"
PAGE_LIMIT = 1000
MAX_RETRIES = 3


def fetch_locations_for_country(iso_code: str, api_key: str) -> list[dict]:
    """
    Paginate through /v3/locations for a single country ISO code.
    Returns the raw 'results' entries, unflattened.
    """
    all_results = []
    page = 1

    while True:
        params = {"iso": iso_code, "limit": PAGE_LIMIT, "page": page}
        headers = {"X-API-Key": api_key}

        response = None
        for attempt in range(MAX_RETRIES):
            response = requests.get(OPENAQ_BASE_URL, params=params, headers=headers)
            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 5))
                print(f"  Rate limited on {iso_code} page {page}, waiting {wait}s...")
                time.sleep(wait)
                continue
            break

        if response is None or response.status_code != 200:
            status = response.status_code if response else "no response"
            print(f"  WARNING: {iso_code} page {page} failed ({status}), skipping")
            break

        payload = response.json()
        results = payload.get("results", [])
        all_results.extend(results)

        found = payload.get("meta", {}).get("found", len(results))
        if page * PAGE_LIMIT >= found or not results:
            break

        page += 1
        time.sleep(0.2)  # be polite between pages

    return all_results


def flatten_location(raw: dict, iso_code: str) -> dict:
    """
    Pull out exactly the fields Phase 3/4/5 need from one raw location
    record. Keeps 'parameters' as a list rather than exploding to
    one-row-per-pollutant — that's a Silver-layer decision, not this one.
    """
    sensors = raw.get("sensors", []) or []
    parameters = sorted({
        s.get("parameter", {}).get("name")
        for s in sensors
        if s.get("parameter", {}).get("name")
    })

    coords = raw.get("coordinates") or {}
    dt_first = raw.get("datetimeFirst") or {}
    dt_last = raw.get("datetimeLast") or {}
    provider = raw.get("provider") or {}
    country = raw.get("country") or {}

    return {
        "location_id": raw.get("id"),
        "name": raw.get("name"),
        "locality": raw.get("locality"),
        "country_code": country.get("code", iso_code),
        "country_name": country.get("name"),
        "provider_id": provider.get("id"),
        "provider_name": provider.get("name"),
        "is_monitor": raw.get("isMonitor"),      # True = reference-grade, False = low-cost
        "is_mobile": raw.get("isMobile"),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "parameters": parameters,
        "datetime_first_utc": dt_first.get("utc"),
        "datetime_last_utc": dt_last.get("utc"),
    }


def discover(countries: list[str], api_key: str) -> list[dict]:
    """Orchestrates fetch + flatten across all target countries."""
    all_locations = []

    for iso_code in countries:
        print(f"Fetching locations for {iso_code}...")
        raw_results = fetch_locations_for_country(iso_code, api_key)
        flattened = [flatten_location(r, iso_code) for r in raw_results]
        all_locations.extend(flattened)
        print(f"  {iso_code}: {len(flattened)} locations found")

    return all_locations


def save_metadata(locations: list[dict], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(locations, f, indent=2)
    print(f"\nSaved {len(locations)} locations to {output_path}")


def print_summary(locations: list[dict]) -> None:
    """Quick sanity check: counts and date coverage per country."""
    by_country = {}
    for loc in locations:
        code = loc["country_code"]
        by_country.setdefault(code, {"count": 0, "earliest": None, "latest": None})
        by_country[code]["count"] += 1

        first = loc.get("datetime_first_utc")
        last = loc.get("datetime_last_utc")
        if first and (by_country[code]["earliest"] is None or first < by_country[code]["earliest"]):
            by_country[code]["earliest"] = first
        if last and (by_country[code]["latest"] is None or last > by_country[code]["latest"]):
            by_country[code]["latest"] = last

    print("\n--- Discovery Summary ---")
    for code, stats in sorted(by_country.items()):
        print(f"{code}: {stats['count']} locations | "
              f"earliest={stats['earliest']} | latest={stats['latest']}")

    providers = sorted({loc["provider_name"] for loc in locations if loc["provider_name"]})
    print(f"\nDistinct providers found: {providers}")


if __name__ == "__main__":
    load_dotenv()
    api_key = os.environ["OPENAQ_API_KEY"]

    locations = discover(TARGET_COUNTRIES, api_key)
    save_metadata(locations, "config/locations_metadata.json")
    print_summary(locations)