from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime

try:  # Python 3.11+
    from datetime import UTC
except ImportError:  # Python <3.11
    from datetime import timezone as _timezone

    UTC = _timezone.utc

import pandas as pd
import pytest

from core.analysis_plan_manager import AnalysisPlanManager


class StubAnalysisPlanModel:
    def __init__(self, plan: Optional[Dict[str, Any]] = None):
        self.plan = plan
        self.created_payload = None

    def create(self, plan_data: Dict[str, Any]) -> Dict[str, Any]:
        self.plan = plan_data
        self.created_payload = plan_data
        return plan_data

    def update(self, plan_id: str, update_data: Dict[str, Any]) -> bool:
        if not self.plan or self.plan["plan_id"] != plan_id:
            return False
        self.plan.update(update_data)
        return True

    def delete(self, plan_id: str) -> bool:
        if self.plan and self.plan["plan_id"] == plan_id:
            self.plan = None
            return True
        return False

    def get_by_id(self, plan_id: str) -> Optional[Dict[str, Any]]:
        if self.plan and self.plan["plan_id"] == plan_id:
            return dict(self.plan)
        return None

    def get_all(self, **_kwargs):
        return [self.plan] if self.plan else []


class StubJoinedQueryStore:
    def __init__(self):
        self.saved_documents = []

    def save_execution(self, document: Dict[str, Any]) -> str:
        self.saved_documents.append(document)
        return "joined-doc-id"


class StubQueryColumnCache:
    def __init__(self, initial: Optional[Dict[str, Dict[str, Any]]] = None):
        self.docs = initial or {}
        self.saved = []

    def get_many(self, query_ids):
        return {qid: self.docs[qid] for qid in query_ids if qid in self.docs}

    def save(
        self,
        *,
        query_id: str,
        columns,
        connector_id: str,
        record_count: Optional[int] = None,
        **_kwargs,
    ) -> None:
        doc = {
            "query_id": query_id,
            "columns": list(columns),
            "connector_id": connector_id,
            "record_count": record_count,
            "updated_at": datetime.now(UTC),
        }
        self.docs[query_id] = doc
        self.saved.append(doc)


class DummyQueryEngine:
    def __init__(
        self,
        stored_queries: Dict[str, Dict[str, Any]],
        column_results: Optional[Dict[str, Dict[str, Any]]] = None,
        dataframe: Optional[pd.DataFrame] = None,
    ):
        self._stored = stored_queries
        self.column_results = column_results or {}
        self.dataframe = dataframe or pd.DataFrame(
            [{"state": "AL", "value": 1}, {"state": "AK", "value": 2}]
        )
        self.analyze_calls = []

    def get_stored_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        stored = self._stored.get(query_id)
        return dict(stored) if stored else None

    def analyze_queries(self, **kwargs):
        self.analyze_calls.append(kwargs)
        return {
            "dataframe": self.dataframe,
            "analysis": {"basic_statistics": {"rows": len(self.dataframe)}},
        }

    def execute_query(
        self,
        source_id: str,
        parameters: Dict[str, Any],
        use_cache: bool = None,
        query_id: str = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ):
        if query_id and query_id in self.column_results:
            return self.column_results[query_id]
        return {
            "success": True,
            "data": {
                "data": [
                    {"state": "AL", "value": 1},
                    {"state": "AK", "value": 2},
                ],
                "metadata": {},
            },
        }


def test_create_plan_delegates_to_model():
    stub_model = StubAnalysisPlanModel()
    manager = AnalysisPlanManager(
        plan_model=stub_model,
        query_engine=DummyQueryEngine({}),
        joined_query_store=StubJoinedQueryStore(),
        query_column_cache=StubQueryColumnCache(),
    )

    payload = {
        "plan_id": "plan-one",
        "plan_name": "Plan One",
        "queries": [
            {"query_id": "q1", "join_column": "state"},
            {"query_id": "q2", "join_column": "state"},
        ],
        "analysis_plan": {"basic_statistics": True},
        "how": "inner",
    }

    created = manager.create_plan(payload)
    assert created["plan_id"] == "plan-one"
    assert stub_model.plan["plan_id"] == "plan-one"


def test_execute_plan_builds_query_specs_with_overrides():
    plan_doc = {
        "plan_id": "education-vs-income",
        "plan_name": "Education vs Income",
        "queries": [
            {
                "query_id": "q1",
                "alias": "census",
                "parameter_overrides": {"year": 2020},
                "join_column": ["state", "year"],
            },
            {"query_id": "q2", "join_column": ["state", "year"]},
        ],
        "how": "left",
        "analysis_plan": {"basic_statistics": True},
    }

    stored_queries = {
        "q1": {
            "connector_id": "census_api",
            "parameters": {"state": "AL", "year": 2019},
            "rename_columns": {"B01001_001E": "total_population"},
        },
        "q2": {
            "connector_id": "usda_nass",
            "parameters": {"state": "AL"},
        },
    }

    stub_model = StubAnalysisPlanModel(plan_doc)
    dummy_engine = DummyQueryEngine(stored_queries)
    store = StubJoinedQueryStore()
    cache = StubQueryColumnCache()
    manager = AnalysisPlanManager(
        plan_model=stub_model,
        query_engine=dummy_engine,
        joined_query_store=store,
        query_column_cache=cache,
    )

    result = manager.execute_plan(
        "education-vs-income",
        parameter_overrides={"q2": {"commodity": "corn"}},
    )

    assert result["plan"]["plan_id"] == "education-vs-income"
    assert len(result["query_specs"]) == 2

    first_spec = result["query_specs"][0]
    assert first_spec["source_id"] == "census_api"
    # Plan-level overrides should replace defaults
    assert first_spec["parameters"]["year"] == 2020
    assert first_spec["rename_columns"]["B01001_001E"] == "total_population"

    second_spec = result["query_specs"][1]
    # Runtime overrides should be applied in addition to stored parameters
    assert second_spec["parameters"]["commodity"] == "corn"
    assert second_spec["parameters"]["state"] == "AL"

    assert dummy_engine.analyze_calls, "QueryEngine.analyze_queries was not invoked"
    call_kwargs = dummy_engine.analyze_calls[0]
    assert call_kwargs["queries"][0]["join_columns"] == ["state", "year"]
    assert call_kwargs["queries"][1]["join_columns"] == ["state", "year"]
    assert call_kwargs["analysis_plan"] == {"basic_statistics": True}
    assert result["joined_query_id"] == "joined-doc-id"
    assert store.saved_documents
    saved = store.saved_documents[0]
    assert saved["plan_id"] == "education-vs-income"
    assert saved["row_count"] == len(dummy_engine.dataframe)
    assert result["plan"]["join_columns"][0]["columns"] == ["state", "year"]


def test_add_analyzer_plan_creates_new_plan_when_missing():
    stub_model = StubAnalysisPlanModel()
    manager = AnalysisPlanManager(
        plan_model=stub_model,
        query_engine=DummyQueryEngine({}),
        joined_query_store=StubJoinedQueryStore(),
        query_column_cache=StubQueryColumnCache(),
    )

    action = manager.add_analyzer_plan(
        plan_id="zip-analysis",
        query_ids=["education_all_levels_by_zip", "household_all_types_by_zip"],
        query_join_columns=["zip code tabulation area", "zip code tabulation area"],
        analysis_plan={"basic_statistics": True},
    )

    assert action == "created"
    assert stub_model.plan is not None
    assert stub_model.plan["plan_name"] == "zip-analysis"
    assert len(stub_model.plan["queries"]) == 2
    assert all(
        entry["join_column"] == "zip code tabulation area"
        for entry in stub_model.plan["queries"]
    )


def test_add_analyzer_plan_updates_existing_plan():
    existing = {
        "plan_id": "zip-analysis",
        "plan_name": "Old Name",
        "queries": [
            {"query_id": "education_all_levels_by_zip", "join_column": "zip code tabulation area"},
            {"query_id": "household_all_types_by_zip", "join_column": "zip code tabulation area"},
        ],
        "analysis_plan": {"basic_statistics": True},
        "how": "inner",
    }
    stub_model = StubAnalysisPlanModel(existing)
    manager = AnalysisPlanManager(
        plan_model=stub_model,
        query_engine=DummyQueryEngine({}),
        joined_query_store=StubJoinedQueryStore(),
        query_column_cache=StubQueryColumnCache(),
    )

    action = manager.add_analyzer_plan(
        plan_id="zip-analysis",
        plan_name="Updated Name",
        description="New description",
        queries=[
            {
                "query_id": "education_all_levels_by_zip",
                "alias": "education",
                "join_column": "zip code tabulation area",
            },
            {
                "query_id": "household_all_types_by_zip",
                "alias": "households",
                "join_column": "zip code tabulation area",
            },
        ],
        analysis_plan={"basic_statistics": True, "exploratory": True},
        how="left",
        tags=["example"],
    )

    assert action == "updated"
    assert stub_model.plan["plan_name"] == "Updated Name"
    assert stub_model.plan["description"] == "New description"
    assert stub_model.plan["queries"][0]["alias"] == "education"
    assert stub_model.plan["how"] == "left"
    assert stub_model.plan["tags"] == ["example"]
    assert stub_model.plan["analysis_plan"]["exploratory"] is True


def test_get_query_columns_uses_cache_and_live_fetch():
    cached_doc = {
        "query_id": "q1",
        "columns": ["state", "population"],
        "connector_id": "census_api",
        "record_count": 25,
        "updated_at": datetime.now(UTC),
    }
    cache = StubQueryColumnCache({"q1": cached_doc})

    stored_queries = {
        "q1": {"connector_id": "census_api", "parameters": {}},
        "q2": {
            "connector_id": "usda_nass",
            "parameters": {},
            "rename_columns": {"Friendly Raw": "friendly_total"},
        },
    }
    column_results = {
        "q2": {
            "success": True,
            "data": {
                "data": [{"state": "AL", "Friendly Raw": 1}],
                "metadata": {
                    "column_name_overrides": {
                        "Friendly Raw": "Friendly Raw",
                    }
                },
                "schema": {"fields": [{"name": "state"}, {"name": "Friendly Raw"}]},
            },
        }
    }

    manager = AnalysisPlanManager(
        plan_model=StubAnalysisPlanModel(),
        query_engine=DummyQueryEngine(
            stored_queries, column_results=column_results
        ),
        joined_query_store=StubJoinedQueryStore(),
        query_column_cache=cache,
    )

    result = manager.get_query_columns(["q1", "q2"])
    assert result["q1"]["cached"] is True
    assert result["q1"]["columns"] == ["state", "population"]

    assert result["q2"]["cached"] is False
    assert "friendly_total" in result["q2"]["columns"]
    assert cache.saved, "live query columns should be cached for next time"
    assert cache.saved[-1]["query_id"] == "q2"


def test_get_query_columns_errors_when_query_missing():
    manager = AnalysisPlanManager(
        plan_model=StubAnalysisPlanModel(),
        query_engine=DummyQueryEngine({}),
        joined_query_store=StubJoinedQueryStore(),
        query_column_cache=StubQueryColumnCache(),
    )

    with pytest.raises(ValueError):
        manager.get_query_columns(["missing-query"])
