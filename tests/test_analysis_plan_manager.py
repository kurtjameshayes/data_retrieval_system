from __future__ import annotations

from typing import Any, Dict, Optional

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


class DummyQueryEngine:
    def __init__(self, stored_queries: Dict[str, Dict[str, Any]]):
        self._stored = stored_queries
        self.analyze_calls = []

    def get_stored_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        stored = self._stored.get(query_id)
        return dict(stored) if stored else None

    def analyze_queries(self, **kwargs):
        self.analyze_calls.append(kwargs)
        return {
            "dataframe": "joined-dataframe",
            "analysis": {"basic_statistics": {"rows": 10}},
        }


def test_create_plan_delegates_to_model():
    stub_model = StubAnalysisPlanModel()
    manager = AnalysisPlanManager(plan_model=stub_model, query_engine=DummyQueryEngine({}))

    payload = {
        "plan_id": "plan-one",
        "plan_name": "Plan One",
        "queries": [
            {"query_id": "q1"},
            {"query_id": "q2"},
        ],
        "join_on": ["state"],
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
            },
            {"query_id": "q2"},
        ],
        "join_on": ["state", "year"],
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
    manager = AnalysisPlanManager(plan_model=stub_model, query_engine=dummy_engine)

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
    assert call_kwargs["join_on"] == ["state", "year"]
    assert call_kwargs["analysis_plan"] == {"basic_statistics": True}


def test_add_analyzer_plan_creates_new_plan_when_missing():
    stub_model = StubAnalysisPlanModel()
    manager = AnalysisPlanManager(plan_model=stub_model, query_engine=DummyQueryEngine({}))

    action = manager.add_analyzer_plan(
        plan_id="zip-analysis",
        query_ids=["education_all_levels_by_zip", "household_all_types_by_zip"],
        join_on=["zip code tabulation area"],
        analysis_plan={"basic_statistics": True},
    )

    assert action == "created"
    assert stub_model.plan is not None
    assert stub_model.plan["plan_name"] == "zip-analysis"
    assert len(stub_model.plan["queries"]) == 2
    assert stub_model.plan["join_on"] == ["zip code tabulation area"]


def test_add_analyzer_plan_updates_existing_plan():
    existing = {
        "plan_id": "zip-analysis",
        "plan_name": "Old Name",
        "queries": [
            {"query_id": "education_all_levels_by_zip"},
            {"query_id": "household_all_types_by_zip"},
        ],
        "join_on": ["zip code tabulation area"],
        "analysis_plan": {"basic_statistics": True},
        "how": "inner",
    }
    stub_model = StubAnalysisPlanModel(existing)
    manager = AnalysisPlanManager(plan_model=stub_model, query_engine=DummyQueryEngine({}))

    action = manager.add_analyzer_plan(
        plan_id="zip-analysis",
        plan_name="Updated Name",
        description="New description",
        queries=[
            {"query_id": "education_all_levels_by_zip", "alias": "education"},
            {"query_id": "household_all_types_by_zip", "alias": "households"},
        ],
        join_on=["zip code tabulation area"],
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
