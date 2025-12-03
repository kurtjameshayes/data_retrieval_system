#!/usr/bin/env python3
"""Example: run analytics on real saved queries backed by live connectors."""
from __future__ import annotations

import argparse
import math
from pprint import pprint
from typing import Any, Dict, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd

from core.query_engine import QueryEngine

FIG_DPI = 160
MAX_IMAGE_DIMENSION = (1 << 16) - 2048  # Matplotlib hard limit is 2**16
PREFERRED_PIXELS_PER_POINT = 6
BASE_FIG_WIDTH = 8
FIG_HEIGHT = 6
MAX_TICKS = 200


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


def build_analysis(
    query_ids: Sequence[str],
    join_on: Sequence[str],
    how: str,
    target_column: str,
    feature_columns: Sequence[str],
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

    analysis_plan = {
        "basic_statistics": True,
        "linear_regression": {
            "features": list(feature_columns),
            "target": target_column,
        },
        "predictive": {
            "features": list(feature_columns),
            "target": target_column,
            "model_type": "forest",
            "n_estimators": 50,
        },
    }

    analysis_result = engine.analyze_queries(
        queries=query_specs,
        join_on=list(join_on),
        analysis_plan=analysis_plan,
        how=how,
    )

    return engine, query_specs, dataframe, analysis_result


def _has_prediction_payload(payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(payload, dict):
        return False
    return all(
        key in payload for key in ("full_predictions", "actual_values", "row_indices")
    )


def _collect_prediction_sources(
    analysis_payload: Dict[str, Any],
) -> List[Tuple[str, str, Dict[str, Any]]]:
    sources: List[Tuple[str, str, Dict[str, Any]]] = []
    if not isinstance(analysis_payload, dict):
        return sources

    linear_metrics = analysis_payload.get("linear_regression")
    if _has_prediction_payload(linear_metrics):
        sources.append(("linear", "Linear Regression", linear_metrics))

    predictive_metrics = analysis_payload.get("predictive_analysis")
    if _has_prediction_payload(predictive_metrics):
        model_label = predictive_metrics.get("model_type", "predictive")
        readable = (model_label or "predictive").replace("_", " ").title()
        sources.append((model_label or "predictive", f"{readable} Model", predictive_metrics))

    forest_metrics = analysis_payload.get("random_forest_regression")
    if _has_prediction_payload(forest_metrics):
        sources.append(("random_forest", "Random Forest Regression", forest_metrics))

    return sources


def _inject_prediction_columns(
    df: pd.DataFrame,
    prediction_sources: List[Tuple[str, str, Dict[str, Any]]],
) -> List[Tuple[str, str]]:
    prediction_columns: List[Tuple[str, str]] = []
    if not prediction_sources:
        return prediction_columns

    index_set = set(df.index)
    for key, label, metrics in prediction_sources:
        column_name = f"pred_{key}".replace(" ", "_")
        df[column_name] = pd.NA
        row_indices = metrics.get("row_indices") or []
        predictions = metrics.get("full_predictions") or []
        for idx, pred in zip(row_indices, predictions):
            if idx in index_set:
                df.at[idx, column_name] = float(pred)
        if df[column_name].notna().any():
            prediction_columns.append((column_name, label))
        else:
            df.drop(columns=[column_name], inplace=True)
    return prediction_columns


def plot_analysis_results(
    dataframe: pd.DataFrame,
    join_on: Sequence[str],
    target_column: str,
    analysis_payload: Dict[str, Any],
) -> bool:
    if target_column not in dataframe.columns:
        raise ValueError(f"Target column '{target_column}' not found in DataFrame")

    if dataframe.empty:
        print("No rows returned from the joined queries; skipping plot generation.")
        return False

    plot_df = dataframe.copy()
    plot_df["_plot_actual"] = pd.to_numeric(plot_df[target_column], errors="coerce")

    prediction_sources = _collect_prediction_sources(analysis_payload)
    prediction_columns = _inject_prediction_columns(plot_df, prediction_sources)
    if not prediction_sources:
        print("No predictive outputs found; plotting actual query data only.")

    valid_mask = plot_df["_plot_actual"].notna()
    if not valid_mask.any():
        print(
            f"Target column '{target_column}' does not contain numeric values; skipping plot."
        )
        return False

    join_columns = [col for col in join_on if col in plot_df.columns]
    if join_columns:
        sort_cols = join_columns + ["_plot_actual"]
        plot_df = plot_df.sort_values(sort_cols)
        plot_df["_join_label"] = (
            plot_df[join_columns].astype(str).agg(" | ".join, axis=1)
        )
        x_label = " | ".join(join_columns)
    else:
        plot_df = plot_df.sort_index()
        plot_df["_join_label"] = plot_df.index.astype(str)
        x_label = "Row index"

    filtered_df = plot_df[valid_mask].copy()
    if filtered_df.empty:
        print("No overlapping numeric rows remained after filtering; skipping plot.")
        return False

    record_count = len(filtered_df)
    desired_width_px = max(
        BASE_FIG_WIDTH * FIG_DPI,
        record_count * PREFERRED_PIXELS_PER_POINT,
    )
    max_width_px = MAX_IMAGE_DIMENSION
    scale_factor = desired_width_px / max_width_px if desired_width_px > max_width_px else 1.0
    scaled_width_px = min(desired_width_px, max_width_px)
    fig_width = scaled_width_px / FIG_DPI
    x_positions = [idx / scale_factor for idx in range(record_count)]
    fig, ax = plt.subplots(figsize=(fig_width, FIG_HEIGHT), dpi=FIG_DPI)
    ax.plot(
        x_positions,
        filtered_df["_plot_actual"],
        label=f"Actual {target_column}",
        marker="o",
    )

    for column_name, legend_label in prediction_columns:
        pred_series = filtered_df[column_name]
        mask = pred_series.notna()
        if mask.any():
            mask_values = mask.to_list()
            x_subset = [x_positions[i] for i, keep in enumerate(mask_values) if keep]
            y_subset = pred_series[mask].tolist()
            ax.plot(
                x_subset,
                y_subset,
                label=f"Predicted ({legend_label})",
                linestyle="--",
                marker="x",
            )

    ax.set_xlabel(x_label)
    ax.set_ylabel(target_column)
    ax.set_title("Query Data vs Predicted Values")
    join_labels = filtered_df["_join_label"].tolist()
    if record_count > MAX_TICKS:
        step = math.ceil(record_count / MAX_TICKS)
        tick_indices = list(range(0, record_count, step))
        if tick_indices[-1] != record_count - 1:
            tick_indices.append(record_count - 1)
    else:
        tick_indices = list(range(record_count))
    tick_positions = [x_positions[idx] for idx in tick_indices]
    tick_labels = [join_labels[idx] for idx in tick_indices]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=45, ha="right")
    ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.7)
    ax.legend()
    fig.tight_layout()

    plt.show()
    plt.close(fig)
    return True


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Join multiple stored queries and run the analysis engine using live APIs."
        )
    )
    parser.add_argument(
        "--query-ids",
        nargs="+",
        required=True,
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
    return parser.parse_args()


def main():
    args = parse_args()

    (
        engine,
        query_specs,
        dataframe,
        analysis,
    ) = build_analysis(
        query_ids=args.query_ids,
        join_on=args.join_on,
        how=args.how,
        target_column=args.target_column,
        feature_columns=args.feature_columns,
    )

    print("Saved queries executed:")
    for idx, query_id in enumerate(args.query_ids):
        stored_query = engine.get_stored_query(query_id)
        alias = query_specs[idx]["alias"]
        connector_id = stored_query["connector_id"] if stored_query else "?"
        print(f"- {alias} ({query_id}) -> {connector_id}")

    print("\nJoined DataFrame sample:\n")
    print(dataframe.head())

    print("\nLinear Regression Summary:\n")
    pprint(analysis["analysis"].get("linear_regression"))

    print("\nPredictive Analysis Summary:\n")
    pprint(analysis["analysis"].get("predictive_analysis"))

    try:
        plot_displayed = plot_analysis_results(
            dataframe=dataframe,
            join_on=args.join_on,
            target_column=args.target_column,
            analysis_payload=analysis["analysis"],
        )
        if plot_displayed:
            print("\nPlot displayed using matplotlib.")
    except Exception as exc:
        print(f"\nUnable to generate plot: {exc}")


if __name__ == "__main__":
    main()
