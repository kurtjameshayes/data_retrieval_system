#!/usr/bin/env python3
"""Command-line helper to execute a stored analysis plan."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from core.analysis_plan_manager import AnalysisPlanManager


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute a stored analysis plan by ID and optionally apply runtime "
            "parameter overrides."
        )
    )
    parser.add_argument(
        "--plan-id",
        required=True,
        help="Identifier of the persisted analysis plan to execute.",
    )
    parser.add_argument(
        "--parameter-overrides",
        help=(
            "JSON string mapping query_id -> overrides. "
            'Example: \'{"query_alpha": {"year": "2023"}}\''
        ),
    )
    parser.add_argument(
        "--overrides-file",
        help="Path to a JSON file containing the parameter overrides payload.",
    )
    parser.add_argument(
        "--use-cache",
        dest="use_cache",
        action="store_true",
        help="Force-enable the query result cache for this run.",
    )
    parser.add_argument(
        "--no-cache",
        dest="use_cache",
        action="store_false",
        help="Disable the query result cache for this run.",
    )
    parser.set_defaults(use_cache=None)
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="Number of joined rows to display after execution (default: 5).",
    )
    parser.add_argument(
        "--output-json",
        help=(
            "Optional path to write the full execution payload (plan metadata, "
            "query specs, dataframe rows, analysis outputs) as JSON."
        ),
    )
    return parser.parse_args(argv)


def _load_parameter_overrides(
    overrides_json: Optional[str], overrides_file: Optional[str]
) -> Optional[Dict[str, Dict[str, Any]]]:
    payload: Optional[str] = None
    if overrides_json:
        payload = overrides_json
    elif overrides_file:
        payload = Path(overrides_file).read_text(encoding="utf-8")

    if payload is None:
        return None

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON for parameter overrides: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("Parameter overrides must be a JSON object.")

    sanitized: Dict[str, Dict[str, Any]] = {}
    for query_id, overrides in parsed.items():
        if not isinstance(overrides, dict):
            raise ValueError(
                f"Overrides for query '{query_id}' must be a JSON object."
            )
        sanitized[str(query_id)] = overrides
    return sanitized or None


def _json_default(value: Any) -> str:
    """Best-effort serializer for numpy/pandas objects."""
    return str(value)


def _print_summary(
    *,
    plan_id: str,
    execution: Dict[str, Any],
    sample_rows: int,
) -> None:
    plan_meta = execution["plan"]
    query_specs = execution["query_specs"]
    dataframe = execution["dataframe"]
    analysis = execution["analysis"]

    print(f"Analysis plan '{plan_id}' executed successfully.\n")
    print("Plan metadata:")
    print(f"  Name: {plan_meta.get('plan_name') or plan_meta.get('plan_id')}")
    print(f"  Description: {plan_meta.get('description') or '-'}")
    join_details = plan_meta.get("join_columns") or []
    if join_details:
        print("  Join columns per query:")
        for entry in join_details:
            label = entry.get("alias") or entry.get("query_id") or "-"
            columns = ", ".join(entry.get("columns") or [])
            print(f"    - {label}: {columns}")
    print(f"  Join type: {plan_meta.get('how', 'inner')}")

    print(f"\nResolved query specs ({len(query_specs)}):")
    for spec in query_specs:
        alias = spec.get("alias") or "<unnamed>"
        print(f"  - {alias} -> {spec.get('source_id')}")

    if hasattr(dataframe, "shape"):
        rows, cols = dataframe.shape
        print(f"\nDataframe shape: {rows} rows x {cols} columns")
        if sample_rows > 0:
            print(f"\nSample ({min(sample_rows, rows)} rows):")
            print(dataframe.head(sample_rows).to_string(index=False))

    if isinstance(analysis, dict):
        print("\nAnalysis sections:")
        if analysis:
            for key in sorted(analysis.keys()):
                print(f"  - {key}")
        else:
            print("  (none)")


def _write_output_json(path: str, execution: Dict[str, Any]) -> None:
    dataframe = execution["dataframe"]
    dataframe_payload: Optional[list[Dict[str, Any]]] = None
    if hasattr(dataframe, "to_dict"):
        dataframe_payload = dataframe.to_dict(orient="records")

    serializable = {
        "plan": execution["plan"],
        "query_specs": execution["query_specs"],
        "dataframe": dataframe_payload,
        "analysis": execution["analysis"],
    }
    Path(path).write_text(
        json.dumps(serializable, indent=2, default=_json_default), encoding="utf-8"
    )
    print(f"\nWrote execution output to {path}")


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        parameter_overrides = _load_parameter_overrides(
            args.parameter_overrides, args.overrides_file
        )
    except ValueError as exc:
        print(f"Error loading parameter overrides: {exc}", file=sys.stderr)
        return 2

    manager = AnalysisPlanManager()
    try:
        execution = manager.execute_plan(
            args.plan_id,
            parameter_overrides=parameter_overrides,
            use_cache=args.use_cache,
        )
    except Exception as exc:  # pragma: no cover - surfaced to CLI user
        print(f"Failed to execute analysis plan '{args.plan_id}': {exc}", file=sys.stderr)
        return 1

    _print_summary(plan_id=args.plan_id, execution=execution, sample_rows=args.sample_rows)

    if args.output_json:
        _write_output_json(args.output_json, execution)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
