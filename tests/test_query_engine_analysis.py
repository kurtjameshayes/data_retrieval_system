import pandas as pd
import pytest

from core.data_analysis import DataAnalysisEngine
from core.query_engine import QueryEngine


class InMemoryStoredQuery:
    def get_by_id(self, query_id):
        return None

    def get_all(self, **_kwargs):
        return []


class FakeConnectorManager:
    def list_sources(self):
        return []


class FakeCacheManager:
    def __init__(self):
        self._store = {}

    def get(self, source_id, parameters):
        return None

    def set(self, source_id, parameters, result, ttl=None, query_id=None):
        self._store[(source_id, tuple(sorted(parameters.items())))] = result
        return True

    def invalidate(self, source_id, parameters=None):
        return 0

    def get_stats(self):
        return {"entries": len(self._store)}


class SampleQueryEngine(QueryEngine):
    def __init__(self, responses):
        super().__init__(
            connector_manager=FakeConnectorManager(),
            cache_manager=FakeCacheManager(),
            analysis_engine=DataAnalysisEngine(),
            stored_query=InMemoryStoredQuery(),
        )
        self._responses = responses

    def execute_query(self, source_id, parameters, use_cache=None, query_id=None, processing_context=None):
        response = self._responses.get(source_id)
        if response is None:
            raise ValueError(f"Missing response for {source_id}")
        return response


@pytest.fixture
def sample_responses():
    return {
        "census_api": {
            "success": True,
            "data": {
                "data": [
                    {"state": "AL", "year": 2020, "population": 10},
                    {"state": "AK", "year": 2020, "population": 20},
                    {"state": "AZ", "year": 2020, "population": 30},
                    {"state": "AR", "year": 2020, "population": 40},
                    {"state": "CA", "year": 2020, "population": 50},
                    {"state": "CO", "year": 2020, "population": 60},
                ]
            },
        },
        "usda_quickstats": {
            "success": True,
            "data": {
                "data": [
                    {"state": "AL", "year": 2020, "corn_value": 1.0},
                    {"state": "AK", "year": 2020, "corn_value": 2.0},
                    {"state": "AZ", "year": 2020, "corn_value": 3.0},
                    {"state": "AR", "year": 2020, "corn_value": 4.0},
                    {"state": "CA", "year": 2020, "corn_value": 5.0},
                    {"state": "CO", "year": 2020, "corn_value": 6.0},
                ]
            },
        },
    }


@pytest.fixture
def renamed_responses():
    return {
        "census_api": {
            "success": True,
            "data": {
                "metadata": {
                    "column_name_overrides": {
                        "B11001_001E": "Total households",
                        "B15002_003E": "Male: No schooling completed",
                    }
                },
                "data": [
                    {
                        "state": "AL",
                        "year": 2020,
                        "Total households": 10,
                        "Male: No schooling completed": 1,
                    },
                    {
                        "state": "AK",
                        "year": 2020,
                        "Total households": 20,
                        "Male: No schooling completed": 2,
                    },
                    {
                        "state": "AZ",
                        "year": 2020,
                        "Total households": 30,
                        "Male: No schooling completed": 3,
                    },
                    {
                        "state": "AR",
                        "year": 2020,
                        "Total households": 40,
                        "Male: No schooling completed": 4,
                    },
                ],
            },
        },
        "support_api": {
            "success": True,
            "data": {
                "data": [
                    {"state": "AL", "year": 2020, "support_value": 100},
                    {"state": "AK", "year": 2020, "support_value": 200},
                    {"state": "AZ", "year": 2020, "support_value": 300},
                    {"state": "AR", "year": 2020, "support_value": 400},
                ]
            },
        },
    }


def test_execute_queries_to_dataframe(sample_responses):
    engine = SampleQueryEngine(sample_responses)
    df = engine.execute_queries_to_dataframe(
        queries=[
            {
                "source_id": "census_api",
                "parameters": {},
                "join_columns": ["state", "year"],
            },
            {
                "source_id": "usda_quickstats",
                "parameters": {},
                "join_columns": ["state", "year"],
            },
        ],
        how="inner",
    )

    assert not df.empty
    assert set(df.columns) == {"state", "year", "population", "corn_value"}
    assert len(df) == 6


def test_analyze_queries_returns_dataframe_and_analysis(sample_responses):
    engine = SampleQueryEngine(sample_responses)
    result = engine.analyze_queries(
        queries=[
            {
                "source_id": "census_api",
                "parameters": {},
                "join_columns": ["state", "year"],
            },
            {
                "source_id": "usda_quickstats",
                "parameters": {},
                "join_columns": ["state", "year"],
            },
        ],
        analysis_plan={
            "basic_statistics": True,
            "linear_regression": {"features": ["corn_value"], "target": "population"},
        },
    )

    dataframe = result["dataframe"]
    analysis = result["analysis"]

    assert isinstance(dataframe, pd.DataFrame)
    assert "linear_regression" in analysis
    assert "coefficients" in analysis["linear_regression"]


def test_dataframe_uses_friendly_names_when_overrides_present(renamed_responses):
    engine = SampleQueryEngine(renamed_responses)
    df = engine.execute_queries_to_dataframe(
        queries=[
            {
                "source_id": "census_api",
                "parameters": {},
                "join_columns": ["state", "year"],
            },
            {
                "source_id": "support_api",
                "parameters": {},
                "join_columns": ["state", "year"],
            },
        ],
    )

    columns = set(df.columns)
    assert {"Total households", "Male: No schooling completed"}.issubset(columns)
    assert "B11001_001E" not in columns
    assert "B15002_003E" not in columns


def test_analysis_plan_can_reference_friendly_names(renamed_responses):
    engine = SampleQueryEngine(renamed_responses)
    result = engine.analyze_queries(
        queries=[
            {
                "source_id": "census_api",
                "parameters": {},
                "join_columns": ["state", "year"],
            },
            {
                "source_id": "support_api",
                "parameters": {},
                "join_columns": ["state", "year"],
            },
        ],
        analysis_plan={
            "linear_regression": {
                "features": ["Male: No schooling completed"],
                "target": "Total households",
            }
        },
    )

    linear = result["analysis"]["linear_regression"]
    assert "coefficients" in linear
    assert "Male: No schooling completed" in linear["coefficients"]
