"""Plotting utilities for analysis outputs."""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import pandas as pd

FIG_DPI = 160
MAX_IMAGE_DIMENSION = (1 << 16) - 2048  # Matplotlib hard limit is 2**16
PREFERRED_PIXELS_PER_POINT = 6
BASE_FIG_WIDTH = 8
FIG_HEIGHT = 6
MAX_TICKS = 200
DEFAULT_MAX_PLOT_POINTS = 1200


class AnalysisPlotter:
    """Encapsulates plotting logic for analysis outputs."""

    def __init__(self, max_plot_points: int = DEFAULT_MAX_PLOT_POINTS) -> None:
        self.max_plot_points = max_plot_points

    def plot(
        self,
        dataframe: pd.DataFrame,
        join_on: Sequence[str],
        target_column: str,
        analysis_payload: Dict[str, Any],
    ) -> bool:
        """Render the analysis plot, returning True if a figure was displayed."""
        if target_column not in dataframe.columns:
            raise ValueError(f"Target column '{target_column}' not found in DataFrame")

        if dataframe.empty:
            print("No rows returned from the joined queries; skipping plot generation.")
            return False

        plot_df = dataframe.copy()
        plot_df["_plot_actual"] = pd.to_numeric(plot_df[target_column], errors="coerce")

        prediction_sources = self._collect_prediction_sources(analysis_payload)
        prediction_columns = self._inject_prediction_columns(plot_df, prediction_sources)
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

        filtered_df = self._downsample_dataframe(filtered_df)
        if filtered_df.empty:
            return False

        record_count = len(filtered_df)
        desired_width_px = max(
            BASE_FIG_WIDTH * FIG_DPI,
            record_count * PREFERRED_PIXELS_PER_POINT,
        )
        max_width_px = MAX_IMAGE_DIMENSION
        scale_factor = (
            desired_width_px / max_width_px if desired_width_px > max_width_px else 1.0
        )
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
        tick_indices = self._select_tick_indices(record_count)
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

    def _downsample_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Reduce the number of rows while retaining the overall trend."""
        record_count = len(dataframe)
        if record_count <= self.max_plot_points:
            return dataframe

        step = math.ceil(record_count / self.max_plot_points)
        indices = list(range(0, record_count, step))
        if indices[-1] != record_count - 1:
            indices.append(record_count - 1)
        return dataframe.iloc[indices].copy()

    @staticmethod
    def _select_tick_indices(record_count: int) -> List[int]:
        if record_count > MAX_TICKS:
            step = math.ceil(record_count / MAX_TICKS)
            tick_indices = list(range(0, record_count, step))
            if tick_indices[-1] != record_count - 1:
                tick_indices.append(record_count - 1)
            return tick_indices
        return list(range(record_count))

    @staticmethod
    def _has_prediction_payload(payload: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(payload, dict):
            return False
        return all(
            key in payload for key in ("full_predictions", "actual_values", "row_indices")
        )

    def _collect_prediction_sources(
        self,
        analysis_payload: Dict[str, Any],
    ) -> List[Tuple[str, str, Dict[str, Any]]]:
        sources: List[Tuple[str, str, Dict[str, Any]]] = []
        if not isinstance(analysis_payload, dict):
            return sources

        linear_metrics = analysis_payload.get("linear_regression")
        if self._has_prediction_payload(linear_metrics):
            sources.append(("linear", "Linear Regression", linear_metrics))

        predictive_metrics = analysis_payload.get("predictive_analysis")
        if self._has_prediction_payload(predictive_metrics):
            model_label = predictive_metrics.get("model_type", "predictive")
            readable = (model_label or "predictive").replace("_", " ").title()
            sources.append(
                (model_label or "predictive", f"{readable} Model", predictive_metrics)
            )

        forest_metrics = analysis_payload.get("random_forest_regression")
        if self._has_prediction_payload(forest_metrics):
            sources.append(("random_forest", "Random Forest Regression", forest_metrics))

        return sources

    @staticmethod
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

