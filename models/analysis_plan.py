"""
Analysis Plan Model

Stores reusable definitions for joining multiple stored queries and
executing predefined analysis plans.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from pymongo import ASCENDING, MongoClient

from config import Config

logger = logging.getLogger(__name__)


class AnalysisPlan:
    """
    Model for persisting analysis plans in MongoDB.

    Schema:
        plan_id (str): Unique identifier for the plan
        plan_name (str): Display name for the plan
        description (str, optional): Human readable summary
        queries (List[Dict]): Ordered list of stored query references
            Each entry supports:
                - query_id (str, required)
                - alias (str, optional)
                - rename_columns (Dict[str, str], optional)
                - parameter_overrides (Dict[str, Any], optional)
        join_on (List[str]): Keys used when joining query results
        how (str): pandas merge strategy (inner, left, right, outer)
        analysis_plan (Dict[str, Any]): Instructions passed to DataAnalysisEngine
        aggregation (Dict[str, Any], optional): Aggregation configuration
        tags (List[str], optional): Metadata labels
        active (bool): Enables or disables the plan
        created_at / updated_at (datetime): Audit timestamps
    """

    VALID_JOIN_STRATEGIES = {"inner", "left", "right", "outer"}

    def __init__(self):
        self.client = MongoClient(Config.MONGO_URI)
        self.db = self.client[Config.DATABASE_NAME]
        self.collection = self.db["analysis_plans"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        """Create indexes for the analysis_plans collection."""
        try:
            self.collection.create_index([("plan_id", ASCENDING)], unique=True)
            self.collection.create_index([("plan_name", ASCENDING)])
            self.collection.create_index([("tags", ASCENDING)])
            self.collection.create_index([("active", ASCENDING)])
        except Exception as exc:
            logger.error("Error creating AnalysisPlan indexes: %s", exc)

    def _validate_required_fields(
        self, plan_data: Dict[str, Any], for_update: bool = False
    ) -> None:
        if not for_update:
            missing = [
                field
                for field in ("plan_id", "plan_name", "queries", "join_on", "analysis_plan")
                if field not in plan_data
            ]
            if missing:
                raise ValueError(f"Missing required fields: {', '.join(missing)}")

        if "queries" in plan_data:
            queries = plan_data["queries"]
            if not isinstance(queries, list) or len(queries) < 2:
                raise ValueError("Analysis plans require at least two stored queries.")
            for entry in queries:
                if not isinstance(entry, dict) or "query_id" not in entry:
                    raise ValueError("Each query entry must be a dict with a query_id.")

        if "join_on" in plan_data:
            if not plan_data["join_on"]:
                raise ValueError("join_on must contain at least one column.")

        if "analysis_plan" in plan_data and not isinstance(plan_data["analysis_plan"], dict):
            raise ValueError("analysis_plan must be a dictionary.")

        if "how" in plan_data:
            how = plan_data["how"]
            if how not in self.VALID_JOIN_STRATEGIES:
                raise ValueError(
                    f"Invalid join strategy '{how}'. "
                    f"Choose from {sorted(self.VALID_JOIN_STRATEGIES)}."
                )

    @staticmethod
    def _normalize_join_keys(join_on: Optional[Sequence[str]]) -> Optional[List[str]]:
        if join_on is None:
            return None
        if isinstance(join_on, str):
            return [join_on]
        if isinstance(join_on, Sequence):
            normalized = [key for key in join_on if key]
            if not normalized:
                raise ValueError("join_on must contain non-empty strings.")
            return list(normalized)
        raise ValueError("join_on must be a string or sequence of strings.")

    @staticmethod
    def _normalize_queries(queries: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
        if queries is None:
            return None
        normalized: List[Dict[str, Any]] = []
        for entry in queries:
            normalized.append(
                {
                    "query_id": entry["query_id"],
                    "alias": entry.get("alias"),
                    "rename_columns": entry.get("rename_columns"),
                    "parameter_overrides": entry.get("parameter_overrides"),
                }
            )
        return normalized

    def _prepare_for_storage(
        self, plan_data: Dict[str, Any], for_update: bool = False
    ) -> Dict[str, Any]:
        self._validate_required_fields(plan_data, for_update=for_update)
        prepared = dict(plan_data)

        if "join_on" in prepared:
            prepared["join_on"] = self._normalize_join_keys(prepared["join_on"])

        if "queries" in prepared:
            prepared["queries"] = self._normalize_queries(prepared["queries"])

        if "how" in prepared:
            prepared["how"] = prepared["how"] or "inner"

        return prepared

    def create(self, plan_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new analysis plan."""
        prepared = self._prepare_for_storage(plan_data, for_update=False)

        now = datetime.utcnow()
        prepared.setdefault("created_at", now)
        prepared.setdefault("updated_at", now)
        prepared.setdefault("active", True)
        prepared.setdefault("tags", [])

        try:
            self.collection.insert_one(prepared)
            prepared.pop("_id", None)
            logger.info("Created analysis plan: %s", prepared["plan_id"])
            return prepared
        except Exception as exc:
            logger.error("Error creating analysis plan %s: %s", prepared.get("plan_id"), exc)
            raise

    def get_by_id(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve an analysis plan by ID."""
        try:
            plan = self.collection.find_one({"plan_id": plan_id})
            if plan:
                plan.pop("_id", None)
            return plan
        except Exception as exc:
            logger.error("Error retrieving analysis plan %s: %s", plan_id, exc)
            return None

    def get_all(
        self, *, active_only: bool = False, tags: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve analysis plans with optional filters."""
        filters: Dict[str, Any] = {}
        if active_only:
            filters["active"] = True
        if tags:
            filters["tags"] = {"$in": tags}

        try:
            plans = list(self.collection.find(filters).sort("plan_name", ASCENDING))
            for plan in plans:
                plan.pop("_id", None)
            return plans
        except Exception as exc:
            logger.error("Error listing analysis plans: %s", exc)
            return []

    def update(self, plan_id: str, update_data: Dict[str, Any]) -> bool:
        """Update an existing analysis plan."""
        if not update_data:
            raise ValueError("update_data must contain at least one field.")

        prepared = self._prepare_for_storage(update_data, for_update=True)
        prepared.pop("plan_id", None)
        prepared.setdefault("updated_at", datetime.utcnow())

        try:
            result = self.collection.update_one({"plan_id": plan_id}, {"$set": prepared})
            if result.modified_count:
                logger.info("Updated analysis plan: %s", plan_id)
                return True
            logger.warning("Analysis plan not updated (missing?): %s", plan_id)
            return False
        except Exception as exc:
            logger.error("Error updating analysis plan %s: %s", plan_id, exc)
            return False

    def delete(self, plan_id: str) -> bool:
        """Delete an analysis plan."""
        try:
            result = self.collection.delete_one({"plan_id": plan_id})
            if result.deleted_count:
                logger.info("Deleted analysis plan: %s", plan_id)
                return True
            logger.warning("No analysis plan deleted (missing?): %s", plan_id)
            return False
        except Exception as exc:
            logger.error("Error deleting analysis plan %s: %s", plan_id, exc)
            return False
