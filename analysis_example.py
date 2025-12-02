#!/usr/bin/env python3
"""
Demonstrates how to join multiple saved query results into a DataFrame and run
analysis without relying on live connectors.
"""
from __future__ import annotations

from copy import deepcopy
from pprint import pprint

from core.data_analysis import DataAnalysisEngine
from core.query_engine import QueryEngine

DEFAULT_SAVED_QUERY_IDS = ["state_population", "state_corn_value"]


class InMemoryStoredQuery:
    """Minimal stored-query stub backed by in-memory demo data."""

    def __init__(self):
        self._queries = self._seed_queries()

    def _seed_queries(self):
        return {
            "state_population": {
                "query_id": "state_population",
                "query_name": "State Population Snapshot",
                "description": "2020 population totals by state.",
                "connector_id": "census_api",
                "parameters": {},
                "alias": "population",
                "tags": ["demo", "census"],
                "active": True,
            },
            "state_corn_value": {
                "query_id": "state_corn_value",
                "query_name": "State Corn Value Snapshot",
                "description": "2020 corn-value totals by state.",
                "connector_id": "usda_quickstats",
                "parameters": {},
                "alias": "agriculture",
                "tags": ["demo", "usda"],
                "active": True,
            },
        }

    def _clone(self, data):
        return deepcopy(data) if data is not None else None

    def get_by_id(self, query_id):
        return self._clone(self._queries.get(query_id))

    def get_all(self, connector_id=None, active_only=False, **_kwargs):
        queries = list(self._queries.values())
        if connector_id:
            queries = [q for q in queries if q["connector_id"] == connector_id]
        if active_only:
            queries = [q for q in queries if q.get("active", True)]
        return [self._clone(q) for q in queries]

    def create(self, data):
        if "query_id" not in data:
            raise ValueError("query_id is required")
        if data["query_id"] in self._queries:
            raise ValueError(f"Query already exists: {data['query_id']}")
        self._queries[data["query_id"]] = deepcopy(data)
        return self._clone(self._queries[data["query_id"]])

    def update(self, query_id, data):
        if query_id not in self._queries:
            return False
        self._queries[query_id].update(data)
        return True

    def delete(self, query_id):
        return self._queries.pop(query_id, None) is not None

    def search(self, search_term):
        if not search_term:
            return []
        term = search_term.lower()
        matches = [
            query
            for query in self._queries.values()
            if term in query.get("query_name", "").lower()
            or term in query.get("description", "").lower()
        ]
        return [self._clone(query) for query in matches]


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


def build_query_specs_from_saved_queries(engine, query_ids):
    """Convert stored queries into specs compatible with QueryEngine helpers."""

    specs = []
    for query_id in query_ids:
        stored_query = engine.get_stored_query(query_id)
        if not stored_query:
            raise ValueError(f"Stored query not found: {query_id}")

        spec = {
            "source_id": stored_query["connector_id"],
            "parameters": stored_query.get("parameters", {}),
            "alias": stored_query.get("alias", query_id),
        }
        if stored_query.get("rename_columns"):
            spec["rename_columns"] = stored_query["rename_columns"]
        specs.append(spec)
    return specs


def build_analysis(engine=None, saved_query_ids=None):
    engine = engine or SampleQueryEngine()
    query_ids = saved_query_ids or DEFAULT_SAVED_QUERY_IDS
    query_specs = build_query_specs_from_saved_queries(engine, query_ids)

    dataframe = engine.execute_queries_to_dataframe(
        queries=query_specs,
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
        queries=query_specs,
        join_on=["state", "year"],
        analysis_plan=analysis_plan,
    )

    return dataframe, analysis_result, query_ids


def main():
    engine = SampleQueryEngine()
    dataframe, analysis, query_ids = build_analysis(engine=engine)

    print("Saved queries used:")
    for query_id in query_ids:
        stored_query = engine.get_stored_query(query_id)
        print(
            f"- {stored_query['query_name']} ({query_id}) -> {stored_query['connector_id']}"
        )

    print("\nJoined DataFrame:\n")
    print(dataframe)

    print("\nLinear Regression Summary:\n")
    pprint(analysis["analysis"]["linear_regression"])

    print("\nPredictive Analysis Summary:\n")
    pprint(analysis["analysis"]["predictive_analysis"])


if __name__ == "__main__":
    main()
