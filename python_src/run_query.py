#!/usr/bin/env python3
"""Execute a single data-retrieval query from the command line."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Sequence

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.query_engine import QueryEngine

DEFAULT_DISPLAY_ROWS = 10


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Execute a single query via QueryEngine and display the first rows "
            "in a plain-text table."
        )
    )
    parser.add_argument(
        "--source-id",
        required=True,
        help="Connector source_id to query (e.g. census_api, usda_quickstats).",
    )
    parser.add_argument(
        "--params-file",
        help="Path to a JSON file containing query parameters.",
    )
    parser.add_argument(
        "--params-json",
        help="Inline JSON string with query parameters.",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=(
            "Individual parameter override using dotted keys "
            "(e.g. filters.state=06). Values are parsed as JSON when possible."
        ),
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_DISPLAY_ROWS,
        help=f"Number of rows to display (default: {DEFAULT_DISPLAY_ROWS}).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass the cache layer for this execution.",
    )
    parser.add_argument(
        "--show-metadata",
        action="store_true",
        help="Print metadata block when returned by the connector.",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Dump the successful response payload as JSON after the table.",
    )
    return parser.parse_args(argv)


def build_parameters(
    *,
    params_file: str | None,
    params_json: str | None,
    inline_params: Iterable[str],
) -> Dict[str, Any]:
    """
    Merge parameter sources in priority order (file -> json -> inline overrides).
    """

    def _merge(target: Dict[str, Any], updates: Dict[str, Any]) -> None:
        for key, value in updates.items():
            if (
                isinstance(value, dict)
                and isinstance(target.get(key), dict)
            ):
                _merge(target[key], value)
            else:
                target[key] = value

    parameters: Dict[str, Any] = {}

    if params_file:
        file_path = os.path.abspath(params_file)
        if not os.path.exists(file_path):
            raise ValueError(f"Parameters file not found: {params_file}")
        with open(file_path, "r", encoding="utf-8") as handle:
            file_payload = json.load(handle)
        if not isinstance(file_payload, dict):
            raise ValueError("Parameters file must contain a JSON object.")
        _merge(parameters, file_payload)

    if params_json:
        try:
            json_payload = json.loads(params_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Unable to parse --params-json: {exc}") from exc
        if not isinstance(json_payload, dict):
            raise ValueError("--params-json must evaluate to a JSON object.")
        _merge(parameters, json_payload)

    for entry in inline_params:
        key, value = _split_key_value(entry)
        _set_nested_value(parameters, key, _parse_inline_value(value))

    return parameters


def _split_key_value(entry: str) -> tuple[str, str]:
    if "=" not in entry:
        raise ValueError(
            f"Inline parameter '{entry}' must use the KEY=VALUE format."
        )
    key, value = entry.split("=", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"Invalid parameter key in '{entry}'.")
    return key, value.strip()


def _parse_inline_value(raw_value: str) -> Any:
    try:
        return json.loads(raw_value)
    except json.JSONDecodeError:
        lowered = raw_value.strip().lower()
        if lowered == "null":
            return None
        return raw_value


def _set_nested_value(target: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    cursor = target
    for part in parts[:-1]:
        if part not in cursor:
            cursor[part] = {}
        elif not isinstance(cursor[part], dict):
            raise ValueError(
                f"Cannot set nested key '{dotted_key}' because '{part}' "
                "already exists and is not an object."
            )
        cursor = cursor[part]
    cursor[parts[-1]] = value


def extract_records(payload: Any) -> List[Dict[str, Any]]:
    """Normalize connector payloads to a list of dict rows."""
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if data is None:
            return []
        if isinstance(data, dict):
            return [data]
    elif isinstance(payload, list):
        return payload
    elif payload:
        return [payload]
    return []


def render_table(records: List[Dict[str, Any]], max_rows: int) -> str:
    """Render the first N records as a simple ASCII table."""
    if not records:
        return "No data returned."

    columns: List[str] = []
    for record in records:
        for key in record.keys():
            if key not in columns:
                columns.append(key)

    rows_to_show = records if max_rows <= 0 else records[:max_rows]

    def stringify(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    widths: Dict[str, int] = {}
    for column in columns:
        widths[column] = len(column)
        for record in rows_to_show:
            cell_value = stringify(record.get(column, ""))
            widths[column] = max(widths[column], len(cell_value))

    header = " | ".join(column.ljust(widths[column]) for column in columns)
    separator = "-+-".join("-" * widths[column] for column in columns)

    lines = [header, separator]
    for record in rows_to_show:
        line = " | ".join(
            stringify(record.get(column, "")).ljust(widths[column])
            for column in columns
        )
        lines.append(line)

    remaining = len(records) - len(rows_to_show)
    if remaining > 0:
        lines.append(f"... {remaining} more row(s)")

    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        parameters = build_parameters(
            params_file=args.params_file,
            params_json=args.params_json,
            inline_params=args.param,
        )
    except ValueError as exc:
        print(f"Parameter error: {exc}", file=sys.stderr)
        return 2

    engine = QueryEngine()
    result = engine.execute_query(
        args.source_id,
        parameters,
        use_cache=None if not args.no_cache else False,
    )

    if not result.get("success"):
        error = result.get("error", "Unknown error")
        print(f"Query failed: {error}", file=sys.stderr)
        return 1

    payload = result.get("data")
    metadata = payload.get("metadata") if isinstance(payload, dict) else {}
    records = extract_records(payload)

    print(f"Source ID     : {args.source_id}")
    print(f"Cache Result  : {result.get('source', 'unknown')}")
    print(f"Rows Returned : {len(records)}")
    if args.show_metadata and isinstance(metadata, dict):
        print("\nMetadata:")
        print(json.dumps(metadata, indent=2, default=str))

    print("\nFirst rows:")
    print(render_table(records, args.rows))

    if args.print_json:
        print("\nFull payload:")
        print(json.dumps(result, indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
