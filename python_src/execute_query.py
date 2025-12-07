#!/usr/bin/env python3
"""Execute a stored query by ID and display its first rows."""

from __future__ import annotations

import argparse
import os
import sys
from typing import Sequence

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.query_engine import QueryEngine
from run_query import DEFAULT_DISPLAY_ROWS, extract_records, render_table


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Configure and parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Execute a stored query via QueryEngine and show the first rows."
    )
    parser.add_argument(
        "query_id",
        help="ID of the stored query to execute.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_DISPLAY_ROWS,
        help=f"Number of rows to display (default: {DEFAULT_DISPLAY_ROWS}).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    engine = QueryEngine()
    result = engine.execute_stored_query(args.query_id)

    if not result.get("success"):
        error_message = result.get("error", "Unknown error")
        print(f"Query failed: {error_message}", file=sys.stderr)
        return 1

    payload = result.get("data")
    records = extract_records(payload)

    print(f"Query ID      : {args.query_id}")
    print(f"Query Name    : {result.get('query_name') or 'N/A'}")
    print(f"Cache Result  : {result.get('source', 'unknown')}")
    print(f"Rows Returned : {len(records)}")

    print("\nFirst rows:")
    print(render_table(records, args.rows))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
