"""
Analysis Plan Manager

Provides helper methods to persist analysis plans and execute them by
delegating to the QueryEngine.
"""

from __future__ import annotations

import logging
from datetime import datetime

try:  # Python 3.11+
    from datetime import UTC
except ImportError:  # Python <3.11
    from datetime import timezone as _timezone

    UTC = _timezone.utc
from typing import Any, Dict, List, Optional, Sequence, Union

import pandas as pd

from core.query_engine import QueryEngine
from models.analysis_plan import AnalysisPlan
from models.joined_query import JoinedQueryStore
from models.query_column_cache import QueryColumnCache

logger = logging.getLogger(__name__)


class AnalysisPlanManager:
    """High-level manager for analysis plan lifecycle and execution."""

    def __init__(
        self,
        plan_model: Optional[AnalysisPlan] = None,
        query_engine: Optional[QueryEngine] = None,
        joined_query_store: Optional[JoinedQueryStore] = None,
        query_column_cache: Optional[QueryColumnCache] = None,
    ):
        self.plan_model = plan_model or AnalysisPlan()
        self.query_engine = query_engine or QueryEngine()
        self.joined_query_store = joined_query_store or JoinedQueryStore()
        self.query_column_cache = query_column_cache or QueryColumnCache()

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

    def add_analyzer_plan(
        self,
        *,
        plan_id: str,
        plan_name: Optional[str] = None,
        description: Optional[str] = None,
        query_ids: Optional[Sequence[str]] = None,
        queries: Optional[Sequence[Dict[str, Any]]] = None,
        query_join_columns: Optional[Sequence[Union[str, Sequence[str]]]] = None,
        analysis_plan: Dict[str, Any],
        how: str = "inner",
        aggregation: Optional[Dict[str, Any]] = None,
        tags: Optional[Sequence[str]] = None,
        active: Optional[bool] = None,
    ) -> str:
        """
        Create or update a reusable analyzer plan definition.

        Returns:
            "created" if a new plan was inserted, otherwise "updated".
        """

        if not isinstance(analysis_plan, dict) or not analysis_plan:
            raise ValueError("analysis_plan must be a non-empty dictionary.")

        if queries and query_ids:
            raise ValueError("Provide either queries or query_ids, not both.")

        normalized_queries: List[Dict[str, Any]] = []
        source_entries = queries if queries is not None else None

        if source_entries is not None:
            for entry in source_entries:
                join_columns = self._normalize_join_columns(
                    entry.get("join_columns") or entry.get("join_column")
                )
                if not join_columns:
                    raise ValueError(
                        f"Query '{entry.get('query_id')}' must define join_column."
                    )
                normalized_queries.append(
                    {
                        "query_id": entry["query_id"],
                        "alias": entry.get("alias"),
                        "rename_columns": entry.get("rename_columns"),
                        "parameter_overrides": entry.get("parameter_overrides"),
                        "join_column": self._format_join_column_for_storage(join_columns),
                    }
                )
        elif query_ids:
            if not query_join_columns or len(query_join_columns) != len(query_ids):
                raise ValueError(
                    "query_join_columns must be provided for each query_id when queries are omitted."
                )
            normalized_queries = [{"query_id": query_id} for query_id in query_ids]
            for idx, entry in enumerate(normalized_queries):
                join_columns = self._normalize_join_columns(query_join_columns[idx])
                if not join_columns:
                    raise ValueError(
                        f"Join column configuration missing for query '{entry['query_id']}'."
                    )
                entry["join_column"] = self._format_join_column_for_storage(join_columns)

        if len(normalized_queries) < 2:
            raise ValueError("Analyzer plans must reference at least two queries.")

        plan_payload: Dict[str, Any] = {
            "plan_id": plan_id,
            "plan_name": plan_name or plan_id,
            "description": description,
            "queries": normalized_queries,
            "how": how or "inner",
            "analysis_plan": analysis_plan,
        }
        if aggregation is not None:
            plan_payload["aggregation"] = aggregation
        if tags is not None:
            plan_payload["tags"] = list(tags)
        if active is not None:
            plan_payload["active"] = active

        existing = self.get_plan(plan_id)
        if existing:
            update_payload = {k: v for k, v in plan_payload.items() if k != "plan_id"}
            self.update_plan(plan_id, update_payload)
            return "updated"

        self.create_plan(plan_payload)
        return "created"

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
            how=plan.get("how", "inner"),
            analysis_plan=plan["analysis_plan"],
            aggregation=plan.get("aggregation"),
            use_cache=use_cache,
        )

        joined_query_id = self._persist_joined_execution(
            plan=plan,
            dataframe=analysis_output.get("dataframe"),
            query_specs=query_specs,
            parameter_overrides=runtime_overrides or None,
        )

        response = {
            "plan": {
                "plan_id": plan["plan_id"],
                "plan_name": plan.get("plan_name"),
                "description": plan.get("description"),
                "join_columns": self._collect_plan_join_columns(plan),
                "how": plan.get("how", "inner"),
                "queries": plan.get("queries", []),
                "analysis_plan": plan.get("analysis_plan"),
            },
            "query_specs": query_specs,
            "dataframe": analysis_output["dataframe"],
            "analysis": analysis_output["analysis"],
        }
        if joined_query_id:
            response["joined_query_id"] = joined_query_id
        return response

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

            join_columns = self._extract_join_columns(query_cfg)

            spec = {
                "source_id": connector_id,
                "parameters": parameters,
                "alias": alias,
            }
            if rename_columns:
                spec["rename_columns"] = rename_columns
            spec["join_columns"] = join_columns

            specs.append(spec)

        if len(specs) < 2:
            raise ValueError("Analysis plans must resolve to at least two query specs.")

        return specs

    def get_query_columns(
        self,
        query_ids: Sequence[str],
        *,
        force_refresh: bool = False,
        use_cache: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Return column metadata for the provided stored queries, leveraging a cache.
        """
        normalized = [str(query_id).strip() for query_id in query_ids if query_id]
        if not normalized:
            return {}

        results: Dict[str, Dict[str, Any]] = {}
        cache_hits: Dict[str, Dict[str, Any]] = {}

        if use_cache and self.query_column_cache:
            cache_hits = self.query_column_cache.get_many(normalized)
            for query_id, doc in cache_hits.items():
                results[query_id] = self._format_cached_columns(doc)

        pending = [qid for qid in normalized if force_refresh or qid not in cache_hits]
        if not pending:
            return results

        for query_id in pending:
            stored_query = self.query_engine.get_stored_query(query_id)
            if not stored_query:
                raise ValueError(f"Stored query '{query_id}' was not found.")

            connector_id = stored_query.get("connector_id")
            if not connector_id:
                raise ValueError(f"Stored query '{query_id}' is missing connector_id.")

            execution = self.query_engine.execute_query(
                connector_id,
                stored_query.get("parameters", {}),
                use_cache=True,
                query_id=query_id,
                processing_context={"stored_query": stored_query},
            )

            if not execution.get("success"):
                error = execution.get("error", "unknown error")
                raise ValueError(f"Unable to load columns for '{query_id}': {error}")

            columns = self._derive_columns_from_result(execution, stored_query)
            record_count = len(QueryEngine._extract_records(execution))
            updated_at = datetime.now(UTC).isoformat()

            results[query_id] = {
                "columns": columns,
                "source_id": connector_id,
                "record_count": record_count,
                "cached": False,
                "updated_at": updated_at,
            }

            if self.query_column_cache:
                try:
                    self.query_column_cache.save(
                        query_id=query_id,
                        columns=columns,
                        connector_id=connector_id,
                        record_count=record_count,
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.warning(
                        "Failed to cache column metadata for query '%s': %s",
                        query_id,
                        exc,
                    )

        return results

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

    @staticmethod
    def _normalize_join_columns(value: Optional[Any]) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            cleaned = value.strip()
            return [cleaned] if cleaned else []
        if isinstance(value, (list, tuple)):
            normalized: List[str] = []
            for entry in value:
                if entry is None:
                    continue
                if isinstance(entry, str):
                    cleaned = entry.strip()
                    if cleaned:
                        normalized.append(cleaned)
                else:
                    normalized.append(str(entry))
            return normalized
        return []

    @staticmethod
    def _format_join_column_for_storage(columns: List[str]) -> Union[str, List[str]]:
        if not columns:
            raise ValueError("join_column requires at least one column name.")
        return columns[0] if len(columns) == 1 else columns

    def _extract_join_columns(self, query_cfg: Dict[str, Any]) -> List[str]:
        join_value = query_cfg.get("join_columns")
        if join_value is None:
            join_value = query_cfg.get("join_column")
        join_columns = self._normalize_join_columns(join_value)
        if not join_columns:
            query_id = query_cfg.get("query_id", "<unknown>")
            raise ValueError(
                f"Query '{query_id}' in analysis plan is missing join_column configuration."
            )
        return join_columns

    def _collect_plan_join_columns(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        join_details: List[Dict[str, Any]] = []
        for entry in plan.get("queries", []) or []:
            join_columns = self._normalize_join_columns(
                entry.get("join_columns") or entry.get("join_column")
            )
            if not join_columns:
                continue
            join_details.append(
                {
                    "query_id": entry.get("query_id"),
                    "alias": entry.get("alias"),
                    "columns": join_columns,
                }
            )
        return join_details

    @staticmethod
    def _format_cached_columns(doc: Dict[str, Any]) -> Dict[str, Any]:
        updated_at = doc.get("updated_at")
        if isinstance(updated_at, datetime):
            updated_at = updated_at.isoformat()
        return {
            "columns": doc.get("columns", []),
            "source_id": doc.get("connector_id"),
            "record_count": doc.get("record_count"),
            "cached": True,
            "updated_at": updated_at,
        }

    def _derive_columns_from_result(
        self,
        execution_result: Dict[str, Any],
        stored_query: Dict[str, Any],
    ) -> List[str]:
        payload = execution_result.get("data")
        records = QueryEngine._extract_records(execution_result)

        if records:
            dataframe = pd.DataFrame(records)
        else:
            dataframe = pd.DataFrame(
                columns=self._extract_schema_columns(payload),
            )

        metadata = QueryEngine._extract_metadata(payload)
        dataframe = QueryEngine._inject_column_aliases(dataframe, metadata)

        rename_map = stored_query.get("rename_columns")
        if rename_map:
            dataframe = dataframe.rename(columns=rename_map)

        return [str(column) for column in dataframe.columns]

    @staticmethod
    def _extract_schema_columns(payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []
        schema = payload.get("schema")
        if not isinstance(schema, dict):
            return []
        fields = schema.get("fields")
        if not isinstance(fields, list):
            return []
        columns: List[str] = []
        for field in fields:
            if isinstance(field, dict):
                name = field.get("name")
                if name:
                    columns.append(name)
        return columns

    def _persist_joined_execution(
        self,
        *,
        plan: Dict[str, Any],
        dataframe: Any,
        query_specs: List[Dict[str, Any]],
        parameter_overrides: Optional[Dict[str, Dict[str, Any]]],
    ) -> Optional[str]:
        if not self.joined_query_store:
            return None
        if not hasattr(dataframe, "to_dict"):
            return None

        try:
            document = self._build_joined_query_document(
                plan=plan,
                dataframe=dataframe,
                query_specs=query_specs,
                parameter_overrides=parameter_overrides,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "Unable to build joined query document for plan '%s': %s",
                plan.get("plan_id"),
                exc,
            )
            return None

        if not document:
            return None

        try:
            return self.joined_query_store.save_execution(document)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "Failed to persist joined query document for plan '%s': %s",
                plan.get("plan_id"),
                exc,
            )
            return None

    def _build_joined_query_document(
        self,
        *,
        plan: Dict[str, Any],
        dataframe: pd.DataFrame,
        query_specs: List[Dict[str, Any]],
        parameter_overrides: Optional[Dict[str, Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(dataframe, pd.DataFrame):
            return None

        records = self._dataframe_to_records(dataframe)
        if records is None:
            return None

        join_details = self._collect_plan_join_columns(plan)
        query_entries = plan.get("queries", []) or []
        query_ids = [
            entry.get("query_id") for entry in query_entries if entry.get("query_id")
        ]
        query_aliases = [
            entry.get("alias") for entry in query_entries if entry.get("alias")
        ]

        document: Dict[str, Any] = {
            "plan_id": plan.get("plan_id"),
            "plan_name": plan.get("plan_name"),
            "description": plan.get("description"),
            "query_ids": query_ids,
            "query_aliases": query_aliases,
            "join_columns": join_details,
            "how": plan.get("how", "inner"),
            "aggregation": plan.get("aggregation"),
            "analysis_plan": plan.get("analysis_plan"),
            "parameter_overrides": parameter_overrides or {},
            "row_count": len(dataframe),
            "column_count": len(dataframe.columns),
            "columns": [str(column) for column in dataframe.columns],
            "data": records,
            "executed_at": datetime.now(UTC),
        }

        if plan.get("tags"):
            document["tags"] = plan.get("tags")

        query_spec_snapshot: List[Dict[str, Any]] = []
        for spec in query_specs:
            snapshot: Dict[str, Any] = {
                "alias": spec.get("alias"),
                "source_id": spec.get("source_id"),
            }
            if spec.get("parameters"):
                snapshot["parameters"] = spec.get("parameters")
            if spec.get("rename_columns"):
                snapshot["rename_columns"] = spec.get("rename_columns")
            query_spec_snapshot.append(snapshot)

        if query_spec_snapshot:
            document["query_specs"] = query_spec_snapshot

        return document

    def _dataframe_to_records(self, dataframe: pd.DataFrame) -> List[Dict[str, Any]]:
        sanitized = dataframe.copy()
        sanitized = sanitized.where(pd.notnull(sanitized), None)
        raw_records = sanitized.to_dict(orient="records")

        return [
            self._normalize_serializable_value(record) for record in raw_records
        ]

    def _normalize_serializable_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(key): self._normalize_serializable_value(sub_value)
                for key, sub_value in value.items()
            }
        if isinstance(value, list):
            return [self._normalize_serializable_value(item) for item in value]
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                pass
        return value
