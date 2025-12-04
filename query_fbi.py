#!/usr/bin/env python3
"""Run stored FBI Crime Data Explorer queries.

This script executes the stored query
`fbi_national_arrests_all_offenses`, which must exist in MongoDB.
Use `python add_fbi_queries.py` to seed the query if it has not yet
been created.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.query_engine import QueryEngine
from models.connector_config import ConnectorConfig

QUERY_ID = "fbi_national_arrests_all_offenses"
DATE_FORMAT = "%m-%Y"


def validate_connector() -> Tuple[bool, str]:
    """Ensure the FBI connector is configured and active."""
    try:
        model = ConnectorConfig()
        config = model.get_by_source_id("fbi_crime")

        if not config:
            return False, "Connector not found. Run: python add_connectors.py fbi_crime"
        if not config.get("active"):
            return False, "Connector is inactive. Update configuration to set active=true"
        if not config.get("api_key"):
            return False, "Connector is missing an API key. Add an API key first"
        return True, "FBI Crime Data connector is ready"
    except Exception as exc:
        return False, f"Error checking connector: {exc}"


def parse_month(value: str) -> str:
    """Validate MM-YYYY input and return the original string."""
    try:
        datetime.strptime(value, DATE_FORMAT)
        return value
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid month '{value}'. Expected format MM-YYYY"
        ) from exc


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute the stored FBI national arrests query"
    )
    parser.add_argument(
        "--from-month",
        dest="from_month",
        type=parse_month,
        help="Start month (MM-YYYY). Overrides stored query parameter."
    )
    parser.add_argument(
        "--to-month",
        dest="to_month",
        type=parse_month,
        help="End month (MM-YYYY). Overrides stored query parameter."
    )
    parser.add_argument(
        "--type",
        dest="result_type",
        choices=["counts", "rates"],
        help="Arrest response type (default from stored query is 'counts')."
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass cached results."
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print the raw JSON payload instead of the formatted summary."
    )
    return parser


def build_parameter_overrides(args: argparse.Namespace) -> Dict[str, str]:
    overrides: Dict[str, str] = {}
    if args.from_month:
        overrides["from"] = args.from_month
    if args.to_month:
        overrides["to"] = args.to_month
    if args.result_type:
        overrides["type"] = args.result_type
    return overrides


def extract_first_series(series_container: Dict) -> Tuple[str, Dict[str, float]]:
    """Return the first key/value pair from a nested dictionary."""
    if not isinstance(series_container, dict) or not series_container:
        return "", {}
    label, values = next(iter(series_container.items()))
    if isinstance(values, dict):
        return label, values
    return label, {}


def month_key(value: str) -> datetime:
    return datetime.strptime(value, DATE_FORMAT)


def format_number(value):
    if value is None:
        return "-"
    if isinstance(value, int):
        return f"{value:,}"
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def display_results(result: Dict[str, Any]):
    print("\n" + "=" * 70)
    print("FBI NATIONAL ARRESTS – STORED QUERY RESULT")
    print("=" * 70)

    if not result.get("success"):
        print(f"\n✗ Query failed: {result.get('error', 'Unknown error')}")
        return

    payload = result.get("data", {})
    metadata = payload.get("metadata", {})
    records = payload.get("data", [])

    print(f"\nQuery: {result.get('query_name', QUERY_ID)}")
    print(f"Source: {result.get('source', 'unknown')}")
    print(f"Records: {metadata.get('record_count', 'N/A')}")

    if not records:
        print("\nNo data returned.")
        return

    record = records[0]
    actual_label, actual_values = extract_first_series(record.get("actuals", {}))
    rate_label, rate_values = extract_first_series(record.get("rates", {}))
    coverage_label, coverage_values = extract_first_series(
        record.get("tooltips", {}).get("Percent of Population Coverage", {})
    )

    month_set = set(actual_values.keys()) | set(rate_values.keys()) | set(coverage_values.keys())
    if not month_set:
        print("\nNo monthly buckets detected in the response.")
        return

    months = sorted(month_set, key=month_key)
    print("\nMonthly arrests summary:")
    header = f"{'Month':<10} {'Arrests':>14} {'Rate/100k':>14} {'Coverage%':>12}"
    print(header)
    print("-" * len(header))

    total_arrests = 0
    for month in months:
        arrests = actual_values.get(month)
        if isinstance(arrests, (int, float)):
            total_arrests += int(arrests)
        rate = rate_values.get(month)
        coverage = coverage_values.get(month)
        print(
            f"{month:<10} {format_number(arrests):>14} "
            f"{format_number(rate):>14} {format_number(coverage):>12}"
        )

    print("\nLabels:")
    if actual_label:
        print(f"  Counts: {actual_label}")
    if rate_label:
        print(f"  Rates: {rate_label}")
    if coverage_label:
        print(f"  Coverage: {coverage_label}")

    print(f"\nTotal arrests across {len(months)} month(s): {format_number(total_arrests)}")
    print("=" * 70 + "\n")


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    ready, status_message = validate_connector()
    print(f"Connector Status: {status_message}")
    if not ready:
        sys.exit(1)

    overrides = build_parameter_overrides(args)
    use_cache = not args.no_cache

    query_engine = QueryEngine()
    result = query_engine.execute_stored_query(
        QUERY_ID,
        use_cache=use_cache,
        parameter_overrides=overrides or None
    )

    if args.raw:
        print(json.dumps(result, indent=2, default=str))
    else:
        display_results(result)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(0)
