"""
Analysis Plan Manager

Provides helper methods to persist analysis plans and execute them by
delegating to the QueryEngine.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.query_engine import QueryEngine
from models.analysis_plan import AnalysisPlan


class AnalysisPlanManager:
    """High-level manager for analysis plan lifecycle and execution."""

    def __init__(
        self,
        plan_model: Optional[AnalysisPlan] = None,
        query_engine: Optional[QueryEngine] = None,
    ):
        self.plan_model = plan_model or AnalysisPlan()
        self.query_engine = query_engine or QueryEngine()

    # -- Persistence helpers -------------------------------------------------

    def create_plan(self, plan_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create and persist a new analysis plan."""
        return self.plan_model.create(plan_data)

    def update_plan(self, plan_id: str, update_data: Dict[str, Any]) -> bool:
        """Update an existing analysis plan."""
        return self.plan_model.update(plan_id, update_data)

    def delete_plan(self, plan_id: str) -> bool:
        """Remove an analysis plan."""
        return self.plan_model.delete(plan_id)

    def get_plan(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """Fetch an analysis plan by ID."""
        return self.plan_model.get_by_id(plan_id)

    def list_plans(self, *, active_only: bool = False) -> List[Dict[str, Any]]:
        """Return available plans."""
        return self.plan_model.get_all(active_only=active_only)

    # -- Execution helpers ---------------------------------------------------

    def execute_plan(
        self,
        plan_id: str,
        *,
        parameter_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
        use_cache: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Execute a persisted analysis plan and return results.

        Args:
            plan_id: Identifier of the plan to execute.
            parameter_overrides: Optional mapping of query_id -> parameter overrides
                                 applied at run-time.
            use_cache: Optional override for query caching.

        Returns:
            Dict containing the persisted plan metadata, generated query specs,
            the merged dataframe, and analysis payload.
        """
        plan = self.get_plan(plan_id)
        if not plan:
            raise ValueError(f"Analysis plan '{plan_id}' was not found.")

        if not plan.get("active", True):
            raise ValueError(f"Analysis plan '{plan_id}' is inactive.")

        runtime_overrides = parameter_overrides or {}
        query_specs = self._build_query_specs(plan, runtime_overrides)

        analysis_output = self.query_engine.analyze_queries(
            queries=query_specs,
            join_on=plan["join_on"],
            how=plan.get("how", "inner"),
            analysis_plan=plan["analysis_plan"],
            aggregation=plan.get("aggregation"),
            use_cache=use_cache,
        )

        return {
            "plan": {
                "plan_id": plan["plan_id"],
                "plan_name": plan.get("plan_name"),
                "description": plan.get("description"),
                "join_on": plan["join_on"],
                "how": plan.get("how", "inner"),
                "queries": plan.get("queries", []),
                "analysis_plan": plan.get("analysis_plan"),
            },
            "query_specs": query_specs,
            "dataframe": analysis_output["dataframe"],
            "analysis": analysis_output["analysis"],
        }

    def _build_query_specs(
        self,
        plan: Dict[str, Any],
        runtime_overrides: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Convert persisted plan definitions into QueryEngine specs."""
        specs: List[Dict[str, Any]] = []
        for query_cfg in plan.get("queries", []):
            query_id = query_cfg["query_id"]
            stored_query = self.query_engine.get_stored_query(query_id)
            if not stored_query:
                raise ValueError(f"Stored query '{query_id}' referenced by plan was not found.")

            connector_id = stored_query.get("connector_id")
            if not connector_id:
                raise ValueError(f"Stored query '{query_id}' is missing connector_id.")

            alias = (
                query_cfg.get("alias")
                or stored_query.get("alias")
                or stored_query.get("query_name")
                or query_id
            )

            parameters = dict(stored_query.get("parameters", {}))
            if query_cfg.get("parameter_overrides"):
                parameters.update(query_cfg["parameter_overrides"])
            if runtime_overrides.get(query_id):
                parameters.update(runtime_overrides[query_id])

            rename_columns = self._merge_rename_maps(
                stored_query.get("rename_columns"),
                query_cfg.get("rename_columns"),
            )

            spec = {
                "source_id": connector_id,
                "parameters": parameters,
                "alias": alias,
            }
            if rename_columns:
                spec["rename_columns"] = rename_columns

            specs.append(spec)

        if len(specs) < 2:
            raise ValueError("Analysis plans must resolve to at least two query specs.")

        return specs

    @staticmethod
    def _merge_rename_maps(
        stored_map: Optional[Dict[str, str]],
        override_map: Optional[Dict[str, str]],
    ) -> Optional[Dict[str, str]]:
        if not stored_map and not override_map:
            return None
        merged: Dict[str, str] = {}
        if isinstance(stored_map, dict):
            merged.update(stored_map)
        if isinstance(override_map, dict):
            merged.update(override_map)
        return merged or None
