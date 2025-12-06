#!/usr/bin/env python3
"""Add analyzer plan definitions derived from analysis_example.py."""
from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, Iterable, List, Sequence

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.analysis_plan_manager import AnalysisPlanManager

DEFAULT_TARGET_COLUMN = (
    "SEX BY EDUCATIONAL ATTAINMENT FOR THE POPULATION 25 YEARS AND OVER: "
    "Estimate!!Total:!!Male:!!No schooling completed"
)
DEFAULT_FEATURE_COLUMNS = [
    "HOUSEHOLD TYPE (INCLUDING LIVING ALONE): Estimate!!Total:",
]
DEFAULT_JOIN_KEYS = ["zip code tabulation area"]


def build_default_analysis_plan(
    *,
    target_column: str,
    feature_columns: Sequence[str],
) -> Dict[str, Any]:
    """
    Mirror analysis_example.build_default_analysis_plan without importing heavy deps.
    """

    features = list(feature_columns)
    return {
        "basic_statistics": True,
        "linear_regression": {
            "features": features,
            "target": target_column,
        },
        "predictive": {
            "features": features,
            "target": target_column,
            "model_type": "forest",
            "n_estimators": 50,
        },
    }

ANALYZER_PLANS = [
    {
        "plan_id": "zip_census_analyzer",
        "plan_name": "ZIP Census Analyzer",
        "description": (
            "Compare educational attainment, household composition, and SNAP participation "
            "by ZIP code using the defaults baked into analysis_example.py."
        ),
        "queries": [
            {
                "query_id": "education_all_levels_by_zip",
                "join_column": DEFAULT_JOIN_KEYS[0],
            },
            {
                "query_id": "household_all_types_by_zip",
                "join_column": DEFAULT_JOIN_KEYS[0],
            },
            {
                "query_id": "snap_all_attributes_by_zip",
                "join_column": DEFAULT_JOIN_KEYS[0],
            },
        ],
        "how": "inner",
        "analysis_plan": build_default_analysis_plan(
            target_column=DEFAULT_TARGET_COLUMN,
            feature_columns=DEFAULT_FEATURE_COLUMNS,
        ),
        "tags": ["analysis-example", "census", "zip"],
    }
]


def _list_plans(plans: Iterable[dict]) -> None:
    print("Analyzer plans available:\n")
    for plan in plans:
        print(f"- {plan['plan_id']} :: {plan['plan_name']}")
        query_labels = [entry["query_id"] for entry in plan.get("queries", [])]
        print(f"  Queries: {', '.join(query_labels)}")
        print("  Join columns:")
        for entry in plan.get("queries", []):
            join_column = entry.get("join_column")
            print(f"    - {entry.get('query_id')}: {join_column}")
        print(f"  Description: {plan['description']}")
        print()


def _apply_plans(plans: Iterable[dict]) -> None:
    manager = AnalysisPlanManager()
    print("Applying analyzer plans...\n")
    for plan in plans:
        try:
            query_ids = plan.get("query_ids")
            queries = plan.get("queries")
            query_join_columns = None
            if not queries and query_ids and plan.get("join_on"):
                query_join_columns = [list(plan["join_on"]) for _ in query_ids]
            action = manager.add_analyzer_plan(
                plan_id=plan["plan_id"],
                plan_name=plan.get("plan_name"),
                description=plan.get("description"),
                query_ids=query_ids,
                queries=queries,
                query_join_columns=query_join_columns,
                how=plan.get("how", "inner"),
                analysis_plan=plan["analysis_plan"],
                aggregation=plan.get("aggregation"),
                tags=plan.get("tags"),
                active=plan.get("active"),
            )
            prefix = "✓" if action == "created" else "↻"
            print(f"{prefix} {action.title()}: {plan['plan_id']}")
        except Exception as exc:  # pragma: no cover - CLI helper
            print(f"✗ Failed: {plan['plan_id']} -> {exc}")
    print()


def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Persist reusable analyzer plans that mirror the defaults in analysis_example.py."
        )
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List plan metadata without writing to MongoDB",
    )
    parser.add_argument(
        "--plan-id",
        action="append",
        dest="plan_ids",
        help="Apply only the specified plan_id (can be repeated)",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    plans: List[dict] = list(ANALYZER_PLANS)

    if args.plan_ids:
        requested = set(args.plan_ids)
        plans = [plan for plan in ANALYZER_PLANS if plan["plan_id"] in requested]
        missing = requested - {plan["plan_id"] for plan in plans}
        if missing:
            print(f"Warning: unknown plan_id values skipped: {', '.join(sorted(missing))}\n")
        if not plans:
            print("No matching analyzer plans to process.")
            return

    if args.list:
        _list_plans(plans)
        return

    _apply_plans(plans)


if __name__ == "__main__":
    main()
