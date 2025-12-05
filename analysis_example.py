#!/usr/bin/env python3
"""Example: run analytics on real saved queries backed by live connectors."""
from __future__ import annotations

import argparse
from pprint import pprint
from typing import Any, Dict, List, Optional, Sequence

from core.analysis_plan_manager import AnalysisPlanManager
from core.analysis_plotter import AnalysisPlotter

from core.query_engine import QueryEngine

SUPPRESSED_ANALYSIS_KEYS = {
    "actual_values",
    "full_predictions",
    "row_indices",
}


def build_query_specs_from_saved_queries(
    engine: QueryEngine, query_ids: Sequence[str]
) -> List[dict]:
    """Convert stored query definitions into QueryEngine-friendly specs."""

    specs: List[dict] = []
    for query_id in query_ids:
        stored_query = engine.get_stored_query(query_id)
        if not stored_query:
            raise ValueError(
                f"Stored query '{query_id}' was not found. "
                "Use manage_queries.py or the API to create it first."
            )

        spec = {
            "source_id": stored_query["connector_id"],
            "parameters": stored_query.get("parameters", {}),
            "alias": stored_query.get("alias")
            or stored_query.get("query_name")
            or query_id,
        }

        rename_columns = stored_query.get("rename_columns")
        if rename_columns:
            spec["rename_columns"] = rename_columns

        specs.append(spec)

    return specs


def build_default_analysis_plan(
    target_column: str,
    feature_columns: Sequence[str],
) -> Dict[str, Any]:
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


def build_analysis(
    query_ids: Sequence[str],
    join_on: Sequence[str],
    how: str,
    analysis_plan: Dict[str, Any],
):
    if len(query_ids) < 2:
        raise ValueError("Provide at least two stored query IDs to build a join.")

    engine = QueryEngine()
    query_specs = build_query_specs_from_saved_queries(engine, query_ids)

    dataframe = engine.execute_queries_to_dataframe(
        queries=query_specs,
        join_on=list(join_on),
        how=how,
    )

    analysis_result = engine.analyze_queries(
        queries=query_specs,
        join_on=list(join_on),
        analysis_plan=analysis_plan,
        how=how,
    )

    return engine, query_specs, dataframe, analysis_result


def persist_analysis_plan(
    *,
    plan_manager: AnalysisPlanManager,
    plan_id: str,
    plan_name: Optional[str],
    description: Optional[str],
    query_ids: Sequence[str],
    join_on: Sequence[str],
    how: str,
    analysis_plan: Dict[str, Any],
) -> str:
    return plan_manager.add_analyzer_plan(
        plan_id=plan_id,
        plan_name=plan_name,
        description=description,
        query_ids=query_ids,
        join_on=join_on,
        how=how,
        analysis_plan=analysis_plan,
    )


def _extract_plan_target(
    analysis_plan: Optional[Dict[str, Any]], default_target: str
) -> str:
    if not isinstance(analysis_plan, dict):
        return default_target
    for key in ("linear_regression", "predictive"):
        block = analysis_plan.get(key)
        if isinstance(block, dict) and isinstance(block.get("target"), str):
            return block["target"]
    return default_target


def _extract_plan_features(
    analysis_plan: Optional[Dict[str, Any]], default_features: Sequence[str]
) -> List[str]:
    if isinstance(analysis_plan, dict):
        for key in ("linear_regression", "predictive"):
            block = analysis_plan.get(key)
            features = block.get("features") if isinstance(block, dict) else None
            if isinstance(features, (list, tuple)) and features:
                return list(features)
    return list(default_features)


def _sanitize_for_display(
    payload: Optional[Dict[str, Any]], suppressed_keys: Sequence[str]
) -> Optional[Dict[str, Any]]:
    """Return a display-friendly copy without verbose list output."""

    def _scrub(value: Any):
        if isinstance(value, dict):
            return {
                key: (
                    f"<omitted list of length {len(sub_value)}>"
                    if key in suppressed_keys and isinstance(sub_value, list)
                    else _scrub(sub_value)
                )
                for key, sub_value in value.items()
            }
        if isinstance(value, list) and len(value) > 25:
            return f"<list of length {len(value)} omitted>"
        return value

    if not isinstance(payload, dict):
        return payload

    return _scrub(payload)


def pprint_predictive_summary(section: Optional[Dict[str, Any]]) -> None:
    """Pretty-print predictive analysis while suppressing long list fields."""
    sanitized = _sanitize_for_display(section, SUPPRESSED_ANALYSIS_KEYS)
    pprint(sanitized)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Join multiple stored queries and run the analysis engine using live APIs."
        )
    )
    parser.add_argument(
        "--query-ids",
        nargs="+",
        help=(
            "Stored query IDs to join. These must already exist in MongoDB. "
            "Use manage_queries.py or add_census_queries.py to create them."
        ),
    )
    parser.add_argument(
        "--join-on",
        nargs="+",
        default=["zip code tabulation area"],
        help="Columns shared across the saved queries used for the join",
    )
    parser.add_argument(
        "--how",
        default="inner",
        choices=["inner", "left", "right", "outer"],
        help="Join strategy passed to pandas.merge",
    )
    parser.add_argument(
        "--target-column",
        default=(
            "SEX BY EDUCATIONAL ATTAINMENT FOR THE POPULATION 25 YEARS AND OVER: "
            "Estimate!!Total:!!Male:!!No schooling completed"
        ),
        help="Column to predict in regression/predictive analyses",
    )
    parser.add_argument(
        "--feature-columns",
        nargs="+",
        default=["HOUSEHOLD TYPE (INCLUDING LIVING ALONE): Estimate!!Total:"],
        help="Feature columns used for modeling",
    )
    parser.add_argument(
        "--analysis-plan-id",
        help="Execute a previously saved analysis plan by ID.",
    )
    parser.add_argument(
        "--save-plan-id",
        help=(
            "Persist the provided CLI configuration as an analysis plan before"
            " executing."
        ),
    )
    parser.add_argument(
        "--plan-name",
        help="Friendly plan name when using --save-plan-id.",
    )
    parser.add_argument(
        "--plan-description",
        help="Optional description stored alongside the saved plan.",
    )

    args = parser.parse_args()
    if not args.analysis_plan_id:
        if not args.query_ids or len(args.query_ids) < 2:
            parser.error(
                "Provide at least two --query-ids when --analysis-plan-id is not supplied."
            )
    if args.save_plan_id and not args.query_ids:
        parser.error("--save-plan-id requires --query-ids to be provided.")
    return args


def main():
    args = parse_args()
    plan_manager: Optional[AnalysisPlanManager] = None
    join_keys = list(args.join_on)
    target_for_plot = args.target_column
    feature_columns_for_plot = list(args.feature_columns)
    executed_query_ids: List[str] = []

    if args.analysis_plan_id:
        plan_manager = AnalysisPlanManager()
        execution = plan_manager.execute_plan(args.analysis_plan_id)
        engine = plan_manager.query_engine
        query_specs = execution["query_specs"]
        dataframe = execution["dataframe"]
        analysis = {"analysis": execution["analysis"]}
        plan_meta = execution["plan"]
        join_keys = plan_meta.get("join_on", join_keys)
        plan_analysis_plan = plan_meta.get("analysis_plan")
        target_for_plot = _extract_plan_target(plan_analysis_plan, target_for_plot)
        feature_columns_for_plot = _extract_plan_features(
            plan_analysis_plan, feature_columns_for_plot
        )
        plan_queries = plan_meta.get("queries", [])
        executed_query_ids = [
            entry["query_id"] for entry in plan_queries if entry.get("query_id")
        ]
        print(f"Executed analysis plan '{args.analysis_plan_id}'.")
    else:
        analysis_plan = build_default_analysis_plan(
            target_column=args.target_column,
            feature_columns=args.feature_columns,
        )
        (
            engine,
            query_specs,
            dataframe,
            analysis,
        ) = build_analysis(
            query_ids=args.query_ids,
            join_on=args.join_on,
            how=args.how,
            analysis_plan=analysis_plan,
        )
        executed_query_ids = list(args.query_ids)

        if args.save_plan_id:
            plan_manager = plan_manager or AnalysisPlanManager()
            action = persist_analysis_plan(
                plan_manager=plan_manager,
                plan_id=args.save_plan_id,
                plan_name=args.plan_name,
                description=args.plan_description,
                query_ids=executed_query_ids,
                join_on=args.join_on,
                how=args.how,
                analysis_plan=analysis_plan,
            )
            print(f"Analysis plan '{args.save_plan_id}' {action} in MongoDB.")

    print("Saved queries executed:")
    for idx, query_id in enumerate(executed_query_ids):
        stored_query = engine.get_stored_query(query_id)
        alias = query_specs[idx]["alias"]
        connector_id = stored_query["connector_id"] if stored_query else "?"
        print(f"- {alias} ({query_id}) -> {connector_id}")

    print("\nJoined DataFrame sample:\n")
    print(dataframe.head())

    print("\nLinear Regression Summary:\n")
    pprint(analysis["analysis"].get("linear_regression"))

    print("\nPredictive Analysis Summary:\n")
    pprint_predictive_summary(analysis["analysis"].get("predictive_analysis"))

    plotter = AnalysisPlotter()
    try:
        plot_displayed = plotter.plot(
            dataframe=dataframe,
            join_on=join_keys,
            target_column=target_for_plot,
            analysis_payload=analysis["analysis"],
            feature_column=feature_columns_for_plot[0]
            if feature_columns_for_plot
            else None,
        )
        if plot_displayed:
            print("\nPlot displayed using matplotlib.")
    except Exception as exc:
        print(f"\nUnable to generate plot: {exc}")


if __name__ == "__main__":
    main()
