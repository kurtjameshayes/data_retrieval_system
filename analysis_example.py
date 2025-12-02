#!/usr/bin/env python3
"""
Demonstrates how to join multiple query results into a DataFrame and run the
analysis suite without relying on live connectors.
"""
from __future__ import annotations

from pprint import pprint

from core.data_analysis import DataAnalysisEngine
from core.query_engine import QueryEngine


class InMemoryStoredQuery:
    """Minimal stored-query stub for demos/tests."""

    def get_by_id(self, query_id):
        return None

    def get_all(self, **_kwargs):
        return []

    def create(self, data):
        return data

    def update(self, query_id, data):
        return False

    def delete(self, query_id):
        return False

    def search(self, search_term):
        return []


class FakeConnectorManager:
    """No-op connector manager used to satisfy QueryEngine dependencies."""

    def list_sources(self):
        return []


class FakeCacheManager:
    """In-memory cache stub for demonstrations."""

    def __init__(self):
        self._store = {}

    def get(self, source_id, parameters):
        return None

    def set(self, source_id, parameters, result, ttl=None, query_id=None):
        key = (source_id, tuple(sorted(parameters.items())))
        self._store[key] = result
        return True

    def invalidate(self, source_id, parameters=None):
        self._store = {
            key: value for key, value in self._store.items() if key[0] != source_id
        }
        return True

    def get_stats(self):
        return {"entries": len(self._store)}


class SampleQueryEngine(QueryEngine):
    """QueryEngine that serves deterministic, in-memory responses."""

    SAMPLE_RESPONSES = {
        "census_api": {
            "success": True,
            "data": {
                "data": [
                    {"state": "AL", "year": 2020, "population": 4903185},
                    {"state": "AK", "year": 2020, "population": 731545},
                    {"state": "AZ", "year": 2020, "population": 7278717},
                    {"state": "AR", "year": 2020, "population": 3017804},
                    {"state": "CA", "year": 2020, "population": 39512223},
                ]
            },
        },
        "usda_quickstats": {
            "success": True,
            "data": {
                "data": [
                    {"state": "AL", "year": 2020, "corn_value": 125.5},
                    {"state": "AK", "year": 2020, "corn_value": 75.2},
                    {"state": "AZ", "year": 2020, "corn_value": 310.8},
                    {"state": "AR", "year": 2020, "corn_value": 255.1},
                    {"state": "CA", "year": 2020, "corn_value": 410.0},
                ]
            },
        },
    }

    def __init__(self):
        super().__init__(
            connector_manager=FakeConnectorManager(),
            cache_manager=FakeCacheManager(),
            analysis_engine=DataAnalysisEngine(),
            stored_query=InMemoryStoredQuery(),
        )

    def execute_query(self, source_id, parameters, use_cache=None, query_id=None):
        try:
            return self.SAMPLE_RESPONSES[source_id]
        except KeyError as exc:
            raise ValueError(f"Unknown source_id: {source_id}") from exc


def build_analysis():
    engine = SampleQueryEngine()

    dataframe = engine.execute_queries_to_dataframe(
        queries=[
            {
                "source_id": "census_api",
                "parameters": {},
                "alias": "population",
            },
            {
                "source_id": "usda_quickstats",
                "parameters": {},
                "alias": "agriculture",
            },
        ],
        join_on=["state", "year"],
        how="inner",
    )

    analysis_plan = {
        "basic_statistics": True,
        "linear_regression": {
            "features": ["corn_value"],
            "target": "population",
        },
        "predictive": {
            "features": ["corn_value"],
            "target": "population",
            "model_type": "forest",
            "n_estimators": 50,
        },
    }

    analysis_result = engine.analyze_queries(
        queries=[
            {"source_id": "census_api", "parameters": {}},
            {"source_id": "usda_quickstats", "parameters": {}},
        ],
        join_on=["state", "year"],
        analysis_plan=analysis_plan,
    )

    return dataframe, analysis_result


def main():
    dataframe, analysis = build_analysis()
    print("\nJoined DataFrame:\n")
    print(dataframe)

    print("\nLinear Regression Summary:\n")
    pprint(analysis["analysis"]["linear_regression"])

    print("\nPredictive Analysis Summary:\n")
    pprint(analysis["analysis"]["predictive_analysis"])


if __name__ == "__main__":
    main()
