#!/usr/bin/env python3
"""Fetch FBI agency metadata for all states and store in MongoDB."""
import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, PyMongoError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import Config  # noqa: E402

STATE_ABBREVIATIONS: Sequence[str] = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
)

API_URL_TEMPLATE = "https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/{state}"
API_KEY_FALLBACK = "iiHnOKfno2Mgkt5AynpvPpUQTEyxE77jo1RU8PIv"
DEFAULT_DELAY = 0.25
DEFAULT_BATCH_SIZE = 500
COLLECTION_NAME = "law_enforcement_agencies"
API_KEY_ENV_VARS = ("FBI_CRIME_API_KEY", "FBI_API_KEY", "FBI_CDE_API_KEY")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch FBI agency metadata for one or more states and store it in MongoDB."
    )
    parser.add_argument(
        "--states",
        help="Comma-separated list of state abbreviations to fetch. Defaults to all.",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        help="Override FBI API key. Defaults to env vars or provided fallback.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Delay (seconds) between API requests. Default: %(default)s",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of upserts per Mongo bulk_write call. Default: %(default)s",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retry attempts per state before giving up. Default: %(default)s",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Drop the target collection before inserting new documents.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and aggregate data without writing to MongoDB.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging verbosity. Default: %(default)s",
    )
    return parser.parse_args()


def resolve_api_key(cli_value: Optional[str]) -> str:
    if cli_value:
        return cli_value
    for env_var in API_KEY_ENV_VARS:
        env_value = os.getenv(env_var)
        if env_value:
            return env_value
    return API_KEY_FALLBACK


def get_states(selection: Optional[str]) -> List[str]:
    if selection:
        requested = [abbr.strip().upper() for abbr in selection.split(",") if abbr.strip()]
        invalid = sorted(set(requested) - set(STATE_ABBREVIATIONS))
        if invalid:
            raise ValueError(f"Invalid state abbreviations: {', '.join(invalid)}")
        return requested
    return list(STATE_ABBREVIATIONS)


def ping_mongo(client: MongoClient) -> None:
    client.admin.command("ping")


def normalize_payload_records(payload: Any) -> List[Dict[str, object]]:
    """Extract the list of agency records from any API payload shape."""
    records: Any
    if isinstance(payload, dict):
        for key in ("results", "data", "items"):
            if key in payload and payload[key] is not None:
                records = payload[key]
                break
        else:
            records = payload
    else:
        records = payload

    if records is None:
        return []
    if isinstance(records, dict):
        return [records]
    if isinstance(records, list):
        return records
    return [records]


def fetch_state_agencies(
    state: str,
    session: requests.Session,
    api_key: str,
    *,
    max_retries: int,
    delay: float,
) -> List[Dict[str, object]]:
    url = API_URL_TEMPLATE.format(state=state)
    params = {"API_KEY": api_key}
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            records = normalize_payload_records(payload)
            logging.info("%s: fetched %d agencies", state, len(records))
            return records
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logging.warning(
                "%s: attempt %d/%d failed (%s)", state, attempt, max_retries, exc
            )
            if attempt < max_retries:
                time.sleep(delay * attempt)

    assert last_error is not None
    raise last_error


def normalize_agency(record: Dict[str, object], state: str, captured_at: datetime) -> Dict[str, object]:
    normalized = dict(record)
    normalized["state_abbr"] = state
    normalized["last_synced_at"] = captured_at
    return normalized


def chunked(iterable: Sequence[Dict[str, object]], size: int) -> Iterable[Sequence[Dict[str, object]]]:
    for idx in range(0, len(iterable), size):
        yield iterable[idx : idx + size]


def upsert_agencies(
    collection: Collection,
    agencies: Sequence[Dict[str, object]],
    batch_size: int,
) -> Tuple[int, int]:
    inserted = 0
    updated = 0

    for batch in chunked(agencies, batch_size):
        operations = []
        for doc in batch:
            ori = doc.get("ori")
            if not ori:
                continue
            operations.append(UpdateOne({"ori": ori}, {"$set": doc}, upsert=True))

        if not operations:
            continue

        try:
            result = collection.bulk_write(operations, ordered=False)
        except BulkWriteError as exc:
            logging.error("Bulk write error: %s", exc.details)
            raise

        inserted += len(result.upserted_ids)
        updated += result.modified_count

    return inserted, updated


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s %(message)s",
    )

    api_key = resolve_api_key(args.api_key)
    if not api_key:
        raise SystemExit("FBI API key is required. Set --api-key or FBI_CRIME_API_KEY env var.")

    try:
        states = get_states(args.states)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    logging.info("Fetching agencies for %d states", len(states))

    captured_at = datetime.utcnow()
    combined: List[Dict[str, object]] = []
    failures: Dict[str, str] = {}
    skipped_without_ori = 0

    with requests.Session() as session:
        for state in states:
            try:
                records = fetch_state_agencies(
                    state,
                    session,
                    api_key,
                    max_retries=args.max_retries,
                    delay=args.delay,
                )
            except Exception as exc:  # noqa: BLE001
                logging.error("%s: failed to fetch agencies (%s)", state, exc)
                failures[state] = str(exc)
                continue

            for record in records:
                normalized = normalize_agency(record, state, captured_at)
                if not normalized.get("ori"):
                    skipped_without_ori += 1
                    continue
                combined.append(normalized)

            time.sleep(args.delay)

    logging.info(
        "Fetched %d agency records (%d failed states, %d skipped missing ORI)",
        len(combined),
        len(failures),
        skipped_without_ori,
    )

    if args.dry_run:
        logging.info("Dry run complete — skipping Mongo writes.")
        if failures:
            logging.warning("States with failures: %s", ", ".join(sorted(failures)))
        return

    try:
        client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
        ping_mongo(client)
        db = client[Config.DATABASE_NAME]
        collection = db[COLLECTION_NAME]
    except PyMongoError as exc:
        raise SystemExit(f"MongoDB connection failed: {exc}") from exc

    if args.replace:
        logging.info("Dropping existing collection '%s'", COLLECTION_NAME)
        collection.drop()

    try:
        inserted, updated = upsert_agencies(collection, combined, args.batch_size)
    except PyMongoError as exc:
        raise SystemExit(f"MongoDB write failed: {exc}") from exc
    finally:
        client.close()

    logging.info(
        "Mongo upsert complete: %d inserted, %d updated, collection='%s'", inserted, updated, COLLECTION_NAME
    )

    if failures:
        logging.warning("States with failures: %s", ", ".join(sorted(failures)))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user.")
        raise SystemExit(1)
