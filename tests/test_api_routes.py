import importlib
import sys
from copy import deepcopy

import pytest


class InMemoryConnectorConfig:
    def __init__(self, *_args, **_kwargs):
        self.storage = {}

    def create(self, config_data):
        source_id = config_data["source_id"]
        self.storage[source_id] = config_data.copy()
        return f"cfg-{source_id}"

    def get_all(self, active_only=True):
        items = list(self.storage.values())
        if active_only:
            return [item for item in items if item.get("active", True)]
        return items

    def update(self, source_id, update_data):
        if source_id not in self.storage:
            return False
        self.storage[source_id].update(update_data)
        return True

    def delete(self, source_id):
        return self.storage.pop(source_id, None) is not None

    def get_by_source_id(self, source_id):
        return self.storage.get(source_id)


class InMemoryQueryResult:
    def __init__(self, *_args, **_kwargs):
        self.storage = {}

    def _key(self, source_id, parameters):
        return (source_id, tuple(sorted(parameters.items())))

    def get(self, source_id, parameters):
        return self.storage.get(self._key(source_id, parameters))

    def save(self, source_id, parameters, result, ttl=None, query_id=None):
        key = self._key(source_id, parameters)
        self.storage[key] = deepcopy(result)
        return "hash"

    def invalidate(self, source_id, parameters=None):
        if parameters:
            key = self._key(source_id, parameters)
            return 1 if self.storage.pop(key, None) else 0
        removed = [key for key in self.storage if key[0] == source_id]
        for key in removed:
            self.storage.pop(key, None)
        return len(removed)

    def get_stats(self):
        return {"entries": len(self.storage)}


class InMemoryStoredQueryRepository:
    def __init__(self):
        self.storage = {}
        self.raise_on_create = None

    def create(self, query_data):
        if self.raise_on_create:
            raise self.raise_on_create
        required = ["query_id", "query_name", "connector_id", "parameters"]
        missing = [field for field in required if field not in query_data]
        if missing:
            raise ValueError(f"Missing required field: {missing[0]}")
        self.storage[query_data["query_id"]] = query_data.copy()
        return query_data

    def get_by_id(self, query_id):
        return self.storage.get(query_id)

    def get_all(self, connector_id=None, active_only=False, tags=None):
        result = list(self.storage.values())
        if connector_id:
            result = [item for item in result if item.get("connector_id") == connector_id]
        if active_only:
            result = [item for item in result if item.get("active", True)]
        if tags:
            tag_set = set(tags)
            result = [item for item in result if tag_set & set(item.get("tags", []))]
        return result

    def update(self, query_id, update_data):
        if query_id not in self.storage:
            return False
        self.storage[query_id].update(update_data)
        return True

    def delete(self, query_id):
        return self.storage.pop(query_id, None) is not None

    def search(self, term):
        term = (term or "").lower()
        return [
            item
            for item in self.storage.values()
            if term in item.get("query_name", "").lower()
            or term in (item.get("description", "").lower())
        ]


class InMemoryAnalysisPlan:
    def __init__(self, *_args, **_kwargs):
        self.storage = {}


class InMemoryJoinedQueryStore:
    def __init__(self, *_args, **_kwargs):
        self.executions = []

    def save_execution(self, document):
        self.executions.append(document)
        return f"exec-{len(self.executions)}"


class InMemoryQueryColumnCache:
    def __init__(self, *_args, **_kwargs):
        self.columns = {}

    def get_many(self, query_ids):
        return {query_id: self.columns[query_id] for query_id in query_ids if query_id in self.columns}

    def save(self, query_id, columns, connector_id, record_count):
        self.columns[query_id] = {
            "columns": columns,
            "connector_id": connector_id,
            "record_count": record_count,
            "updated_at": "now",
        }


def _import_routes_with_inmemory_dependencies():
    import core.analysis_plan_manager as analysis_plan_module
    import core.cache_manager as cache_module
    import core.connector_manager as connector_module
    import core.query_engine as query_engine_module
    import models.connector_config as connector_config_module

    patches = [
        (connector_config_module, "ConnectorConfig", InMemoryConnectorConfig),
        (connector_module, "ConnectorConfig", InMemoryConnectorConfig),
        (cache_module, "QueryResult", InMemoryQueryResult),
        (query_engine_module, "StoredQuery", InMemoryStoredQueryRepository),
        (analysis_plan_module, "AnalysisPlan", InMemoryAnalysisPlan),
        (analysis_plan_module, "JoinedQueryStore", InMemoryJoinedQueryStore),
        (analysis_plan_module, "QueryColumnCache", InMemoryQueryColumnCache),
    ]

    originals = []
    for module, attr, stub in patches:
        originals.append((module, attr, getattr(module, attr)))
        setattr(module, attr, stub)

    try:
        sys.modules.pop("api.routes", None)
        routes_module = importlib.import_module("api.routes")
    finally:
        for module, attr, original in reversed(originals):
            setattr(module, attr, original)

    routes_module.app.config["TESTING"] = True
    return routes_module


routes = _import_routes_with_inmemory_dependencies()


class StubConnector:
    def __init__(self, capabilities=None, connected=True):
        self.capabilities = capabilities or {}
        self.connected = connected
        self.disconnected = False

    def get_capabilities(self):
        return self.capabilities

    def disconnect(self):
        self.disconnected = True

    def process_query_result(self, payload, context):
        return payload


class StubConnectorManager:
    def __init__(self):
        self.connectors = {}
        self.sources = []
        self.list_exception = None
        self.get_exception = None
        self.load_calls = 0

    def load_connectors(self):
        self.load_calls += 1

    def list_sources(self):
        if self.list_exception:
            raise self.list_exception
        if self.sources:
            return self.sources
        result = []
        for source_id, connector in self.connectors.items():
            result.append(
                {
                    "source_id": source_id,
                    "capabilities": connector.get_capabilities(),
                    "connected": connector.connected,
                }
            )
        return result

    def get_connector(self, source_id):
        if self.get_exception:
            raise self.get_exception
        return self.connectors.get(source_id)


class StubCacheManager:
    def __init__(self):
        self.stats = {"entries": 0}
        self.invalidated = []

    def get_stats(self):
        return self.stats

    def invalidate(self, source_id):
        self.invalidated.append(source_id)
        return 1


class StubAnalysisPlanManager:
    def __init__(self):
        self.columns_response = {}
        self.raise_error = None
        self.last_request = None

    def get_query_columns(self, query_ids, force_refresh=False):
        self.last_request = {
            "query_ids": list(query_ids) if query_ids is not None else None,
            "force_refresh": force_refresh,
        }
        if self.raise_error:
            raise self.raise_error
        return self.columns_response


class StubQueryEngine:
    def __init__(self):
        self.stats_payload = {"cache_stats": {}, "available_sources": 0}
        self.execute_query_response = {"success": True, "data": {}}
        self.execute_multi_response = [{"success": True}]
        self.validation_response = {"valid": True}
        self.execute_stored_query_result = {"success": True, "data": {}}
        self.stored_query = InMemoryStoredQueryRepository()
        self.raise_on_stats = None
        self.last_execute_query = None
        self.last_multi_query = None
        self.last_validation = None
        self.last_execute_stored_query = None

    def get_query_stats(self):
        if self.raise_on_stats:
            raise self.raise_on_stats
        return self.stats_payload

    def execute_query(self, source_id, parameters, use_cache):
        self.last_execute_query = {
            "source_id": source_id,
            "parameters": deepcopy(parameters),
            "use_cache": use_cache,
        }
        return deepcopy(self.execute_query_response)

    def execute_multi_source_query(self, queries, use_cache):
        self.last_multi_query = {
            "queries": deepcopy(queries),
            "use_cache": use_cache,
        }
        return deepcopy(self.execute_multi_response)

    def validate_query(self, source_id, parameters):
        self.last_validation = {
            "source_id": source_id,
            "parameters": deepcopy(parameters),
        }
        return deepcopy(self.validation_response)

    def execute_stored_query(self, query_id, use_cache=True, parameter_overrides=None):
        self.last_execute_stored_query = {
            "query_id": query_id,
            "use_cache": use_cache,
            "parameter_overrides": deepcopy(parameter_overrides),
        }
        return deepcopy(self.execute_stored_query_result)

    def get_stored_query(self, query_id):
        return self.stored_query.get_by_id(query_id)


class ApiTestContext:
    def __init__(self, monkeypatch):
        self.config_model = InMemoryConnectorConfig()
        self.connector_manager = StubConnectorManager()
        self.cache_manager = StubCacheManager()
        self.query_engine = StubQueryEngine()
        self.analysis_plan_manager = StubAnalysisPlanManager()

        monkeypatch.setattr(routes, "config_model", self.config_model)
        monkeypatch.setattr(routes, "connector_manager", self.connector_manager)
        monkeypatch.setattr(routes, "cache_manager", self.cache_manager)
        monkeypatch.setattr(routes, "query_engine", self.query_engine)
        monkeypatch.setattr(routes, "analysis_plan_manager", self.analysis_plan_manager)

        self.client = routes.app.test_client()


@pytest.fixture
def api_context(monkeypatch):
    return ApiTestContext(monkeypatch)


def test_health_check_returns_stats(api_context):
    api_context.query_engine.stats_payload = {"requests": 5}

    response = api_context.client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.get_json()["stats"] == {"requests": 5}


def test_health_check_handles_engine_error(api_context):
    api_context.query_engine.raise_on_stats = RuntimeError("boom")

    response = api_context.client.get("/api/v1/health")

    assert response.status_code == 500
    assert response.get_json()["status"] == "unhealthy"


def test_list_sources_success(api_context):
    api_context.connector_manager.sources = [
        {"source_id": "alpha", "capabilities": {"fields": ["id"]}, "connected": True}
    ]

    response = api_context.client.get("/api/v1/sources")

    assert response.status_code == 200
    assert response.get_json()["sources"][0]["source_id"] == "alpha"


def test_list_sources_failure_returns_500(api_context):
    api_context.connector_manager.list_exception = RuntimeError("error")

    response = api_context.client.get("/api/v1/sources")

    assert response.status_code == 500
    assert response.get_json()["success"] is False


def test_get_source_info_success(api_context):
    api_context.connector_manager.connectors["alpha"] = StubConnector({"can_join": True})

    response = api_context.client.get("/api/v1/sources/alpha")

    body = response.get_json()
    assert response.status_code == 200
    assert body["source_id"] == "alpha"
    assert body["capabilities"] == {"can_join": True}


def test_get_source_info_missing_returns_404(api_context):
    response = api_context.client.get("/api/v1/sources/missing")

    assert response.status_code == 404
    assert response.get_json()["success"] is False


def test_create_source_requires_body(api_context):
    response = api_context.client.post(
        "/api/v1/sources",
        data="null",
        content_type="application/json",
    )

    assert response.status_code == 400
    assert "Request body" in response.get_json()["error"]


def test_create_source_validates_required_fields(api_context):
    response = api_context.client.post("/api/v1/sources", json={"source_id": "alpha"})

    assert response.status_code == 400
    assert "Missing required field" in response.get_json()["error"]


def test_create_source_succeeds(api_context):
    payload = {
        "source_id": "alpha",
        "source_name": "Alpha",
        "connector_type": "local_file",
    }

    response = api_context.client.post("/api/v1/sources", json=payload)

    assert response.status_code == 201
    assert api_context.connector_manager.load_calls == 1
    assert api_context.config_model.get_by_source_id("alpha") is not None


def test_update_source_requires_body(api_context):
    response = api_context.client.put(
        "/api/v1/sources/alpha",
        data="null",
        content_type="application/json",
    )

    assert response.status_code == 400
    assert "Request body" in response.get_json()["error"]


def test_update_source_not_found(api_context):
    response = api_context.client.put("/api/v1/sources/alpha", json={"source_name": "Alpha"})

    assert response.status_code == 404


def test_update_source_success_reloads_connectors(api_context):
    api_context.config_model.create(
        {"source_id": "alpha", "source_name": "Alpha", "connector_type": "local_file"}
    )

    response = api_context.client.put("/api/v1/sources/alpha", json={"source_name": "Beta"})

    assert response.status_code == 200
    assert api_context.connector_manager.load_calls == 1
    assert api_context.config_model.get_by_source_id("alpha")["source_name"] == "Beta"


def test_delete_source_success_disconnects_connector(api_context):
    api_context.config_model.create(
        {"source_id": "alpha", "source_name": "Alpha", "connector_type": "local_file"}
    )
    connector = StubConnector()
    api_context.connector_manager.connectors["alpha"] = connector

    response = api_context.client.delete("/api/v1/sources/alpha")

    assert response.status_code == 200
    assert "alpha" not in api_context.connector_manager.connectors
    assert connector.disconnected is True


def test_delete_source_not_found_returns_404(api_context):
    response = api_context.client.delete("/api/v1/sources/missing")

    assert response.status_code == 404


def test_execute_query_requires_body(api_context):
    response = api_context.client.post(
        "/api/v1/query",
        data="null",
        content_type="application/json",
    )

    assert response.status_code == 400


def test_execute_query_requires_source_parameter(api_context):
    response = api_context.client.post("/api/v1/query", json={"filters": {}})

    assert response.status_code == 400
    assert "source parameter" in response.get_json()["error"]


def test_execute_query_maps_optional_parameters(api_context):
    payload = {
        "source": "alpha",
        "filters": {"state": "CA"},
        "fields": ["state", "value"],
        "limit": 10,
        "offset": 5,
        "use_cache": False,
    }

    response = api_context.client.post("/api/v1/query", json=payload)

    assert response.status_code == 200
    recorded = api_context.query_engine.last_execute_query
    assert recorded["source_id"] == "alpha"
    assert recorded["parameters"]["columns"] == ["state", "value"]
    assert recorded["parameters"]["limit"] == 10
    assert recorded["parameters"]["offset"] == 5
    assert recorded["use_cache"] is False


def test_execute_query_failure_returns_400(api_context):
    api_context.query_engine.execute_query_response = {"success": False, "error": "boom"}

    response = api_context.client.post(
        "/api/v1/query",
        json={"source": "alpha", "filters": {}},
    )

    assert response.status_code == 400
    assert response.get_json()["error"] == "boom"


def test_execute_multi_query_requires_body(api_context):
    response = api_context.client.post(
        "/api/v1/query/multi",
        data="null",
        content_type="application/json",
    )

    assert response.status_code == 400


def test_execute_multi_query_requires_queries(api_context):
    response = api_context.client.post("/api/v1/query/multi", json={"queries": []})

    assert response.status_code == 400


def test_execute_multi_query_success(api_context):
    payload = {
        "queries": [
            {"source_id": "alpha", "parameters": {"state": "CA"}},
            {"source_id": "beta", "parameters": {}},
        ],
        "use_cache": False,
    }

    response = api_context.client.post("/api/v1/query/multi", json=payload)

    assert response.status_code == 200
    assert api_context.query_engine.last_multi_query["use_cache"] is False
    assert len(response.get_json()["results"]) == 1


def test_validate_query_requires_body(api_context):
    response = api_context.client.post(
        "/api/v1/query/validate",
        data="null",
        content_type="application/json",
    )

    assert response.status_code == 400


def test_validate_query_requires_source(api_context):
    response = api_context.client.post("/api/v1/query/validate", json={"filters": {}})

    assert response.status_code == 400


def test_validate_query_success(api_context):
    payload = {"source": "alpha", "filters": {"state": "CA"}}
    api_context.query_engine.validation_response = {"valid": True, "source_id": "alpha"}

    response = api_context.client.post("/api/v1/query/validate", json=payload)

    assert response.status_code == 200
    assert response.get_json()["valid"] is True
    assert api_context.query_engine.last_validation["source_id"] == "alpha"


def test_cache_stats_endpoint_returns_payload(api_context):
    api_context.cache_manager.stats = {"entries": 3}

    response = api_context.client.get("/api/v1/cache/stats")

    assert response.status_code == 200
    assert response.get_json()["stats"] == {"entries": 3}


def test_cache_invalidate_endpoint(api_context):
    response = api_context.client.delete("/api/v1/cache/source-1")

    assert response.status_code == 200
    assert api_context.cache_manager.invalidated == ["source-1"]


def test_create_stored_query_missing_fields(api_context):
    payload = {
        "query_id": "q1",
        "query_name": "Query 1",
        "connector_id": "alpha",
    }

    response = api_context.client.post("/api/v1/queries", json=payload)

    assert response.status_code == 400


def test_create_stored_query_value_error(api_context):
    api_context.query_engine.stored_query.raise_on_create = ValueError("invalid")
    payload = {
        "query_id": "q1",
        "query_name": "Query 1",
        "connector_id": "alpha",
        "parameters": {},
    }

    response = api_context.client.post("/api/v1/queries", json=payload)

    assert response.status_code == 400
    assert "invalid" in response.get_json()["error"]


def test_create_stored_query_success(api_context):
    payload = {
        "query_id": "q1",
        "query_name": "Query 1",
        "connector_id": "alpha",
        "parameters": {},
    }

    response = api_context.client.post("/api/v1/queries", json=payload)

    assert response.status_code == 201
    assert response.get_json()["query_id"] == "q1"


def test_list_stored_queries_filters_by_params(api_context):
    api_context.query_engine.stored_query.create(
        {
            "query_id": "q1",
            "query_name": "Alpha",
            "connector_id": "c1",
            "parameters": {},
            "tags": ["finance"],
            "active": True,
        }
    )
    api_context.query_engine.stored_query.create(
        {
            "query_id": "q2",
            "query_name": "Beta",
            "connector_id": "c2",
            "parameters": {},
            "tags": ["ops"],
            "active": False,
        }
    )

    response = api_context.client.get(
        "/api/v1/queries",
        query_string={"connector_id": "c1", "active_only": "true", "tags": ["finance"]},
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data["count"] == 1
    assert data["queries"][0]["query_id"] == "q1"


def test_get_stored_query_not_found(api_context):
    response = api_context.client.get("/api/v1/queries/missing")

    assert response.status_code == 404


def test_get_stored_query_success(api_context):
    api_context.query_engine.stored_query.create(
        {
            "query_id": "q1",
            "query_name": "Alpha",
            "connector_id": "c1",
            "parameters": {},
        }
    )

    response = api_context.client.get("/api/v1/queries/q1")

    assert response.status_code == 200
    assert response.get_json()["query"]["query_id"] == "q1"


def test_update_stored_query_success(api_context):
    api_context.query_engine.stored_query.create(
        {
            "query_id": "q1",
            "query_name": "Alpha",
            "connector_id": "c1",
            "parameters": {},
        }
    )

    response = api_context.client.put("/api/v1/queries/q1", json={"description": "updated"})

    assert response.status_code == 200


def test_update_stored_query_not_found(api_context):
    response = api_context.client.put("/api/v1/queries/missing", json={"description": "updated"})

    assert response.status_code == 404


def test_delete_stored_query_success(api_context):
    api_context.query_engine.stored_query.create(
        {
            "query_id": "q1",
            "query_name": "Alpha",
            "connector_id": "c1",
            "parameters": {},
        }
    )

    response = api_context.client.delete("/api/v1/queries/q1")

    assert response.status_code == 200
    assert "q1" not in api_context.query_engine.stored_query.storage


def test_delete_stored_query_not_found(api_context):
    response = api_context.client.delete("/api/v1/queries/missing")

    assert response.status_code == 404


def test_execute_stored_query_success(api_context):
    api_context.query_engine.execute_stored_query_result = {"success": True, "data": [1]}

    response = api_context.client.post("/api/v1/queries/q1/execute", json={"use_cache": False})

    assert response.status_code == 200
    assert api_context.query_engine.last_execute_stored_query["use_cache"] is False


def test_execute_stored_query_failure_returns_400(api_context):
    api_context.query_engine.execute_stored_query_result = {"success": False, "error": "bad"}

    response = api_context.client.post("/api/v1/queries/q1/execute", json={})

    assert response.status_code == 400
    assert response.get_json()["error"] == "bad"


def test_search_stored_queries_requires_term(api_context):
    response = api_context.client.get("/api/v1/queries/search")

    assert response.status_code == 400


def test_search_stored_queries_returns_matches(api_context):
    api_context.query_engine.stored_query.create(
        {
            "query_id": "q1",
            "query_name": "Population",
            "connector_id": "c1",
            "parameters": {},
            "description": "Population totals",
        }
    )

    response = api_context.client.get("/api/v1/queries/search", query_string={"q": "pop"})

    assert response.status_code == 200
    assert response.get_json()["count"] == 1


def test_analysis_plan_query_columns_validates_payload(api_context):
    response = api_context.client.post(
        "/api/v1/analysis-plans/query-columns",
        json={"query_ids": "invalid"},
    )

    assert response.status_code == 400


def test_analysis_plan_query_columns_handles_value_error(api_context):
    api_context.analysis_plan_manager.raise_error = ValueError("bad columns")

    response = api_context.client.post(
        "/api/v1/analysis-plans/query-columns",
        json={"query_ids": ["q1"]},
    )

    assert response.status_code == 400
    assert "bad columns" in response.get_json()["error"]


def test_analysis_plan_query_columns_success(api_context):
    api_context.analysis_plan_manager.columns_response = {
        "q1": {"columns": ["state"], "cached": True}
    }

    response = api_context.client.post(
        "/api/v1/analysis-plans/query-columns",
        json={"query_ids": ["q1"], "force_refresh": True},
    )

    assert response.status_code == 200
    assert response.get_json()["columns"]["q1"]["columns"] == ["state"]
    assert api_context.analysis_plan_manager.last_request == {
        "query_ids": ["q1"],
        "force_refresh": True,
    }


def test_not_found_handler_returns_json(api_context):
    response = api_context.client.get("/does-not-exist")

    assert response.status_code == 404
    assert response.get_json()["error"] == "Endpoint not found"