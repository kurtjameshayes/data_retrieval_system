import copy

from connectors.census.connector import CensusConnector
from core.base_connector import BaseConnector
from core.query_engine import QueryEngine
from core.data_analysis import DataAnalysisEngine


class FakeAttrRepo:
    def __init__(self, mapping):
        self.mapping = mapping
    
    def get_descriptions(self, dataset, attribute_codes):
        return {
            code: self.mapping.get(code)
            for code in attribute_codes
            if self.mapping.get(code)
        }


class DummyStoredQuery:
    def get_by_id(self, query_id):
        return None
    
    def get_all(self, **kwargs):
        return []


def test_census_connector_process_query_result_replaces_codes():
    connector = CensusConnector({"source_id": "census_api", "source_name": "Census"})
    connector._attribute_repository = FakeAttrRepo({
        "B22010_001E": "Total households",
        "B22010_002E": "SNAP households",
    })
    
    result = {
        "metadata": {},
        "data": [
            {"NAME": "ZCTA 12345", "B22010_001E": "10", "B22010_002E": "5"},
        ],
        "schema": {
            "fields": [
                {"name": "NAME", "type": "string"},
                {"name": "B22010_001E", "type": "string"},
                {"name": "B22010_002E", "type": "string"},
            ]
        },
    }
    context = {"parameters": {"dataset": "2022/acs/acs5"}}
    
    processed = connector.process_query_result(copy.deepcopy(result), context)
    
    record = processed["data"][0]
    assert "Total households" in record
    assert "SNAP households" in record
    assert "B22010_001E" not in record
    
    fields = processed["schema"]["fields"]
    assert any(field["name"] == "Total households" for field in fields)
    assert any(field["name"] == "SNAP households" for field in fields)
    
    overrides = processed["metadata"]["column_name_overrides"]
    assert overrides["B22010_001E"] == "Total households"
    assert overrides["B22010_002E"] == "SNAP households"


class StubConnector(BaseConnector):
    def __init__(self):
        super().__init__({"source_id": "stub_connector", "source_name": "Stub"})
        self.connected = True
        self.process_calls = 0
    
    def connect(self) -> bool:
        self.connected = True
        return True
    
    def disconnect(self) -> bool:
        self.connected = False
        return True
    
    def validate(self) -> bool:
        return True
    
    def query(self, parameters):
        return {
            "metadata": {},
            "data": [],
            "schema": {"fields": []},
        }
    
    def transform(self, data):
        return data
    
    def process_query_result(self, payload, context=None):
        self.process_calls += 1
        record = payload["data"][0]
        if "renamed" not in record:
            record["renamed"] = record.pop("code")
            payload["schema"]["fields"][0]["name"] = "renamed"
        return payload


class StubConnectorManager:
    def __init__(self, connector, payload):
        self._connector = connector
        self._payload = payload
    
    def query(self, source_id, parameters):
        return {
            "success": True,
            "data": copy.deepcopy(self._payload),
            "source_id": source_id,
        }
    
    def get_connector(self, source_id):
        return self._connector
    
    def list_sources(self):
        return []


class StubCacheManager:
    def __init__(self):
        self._store = {}
    
    def _make_key(self, source_id, parameters):
        return (source_id, tuple(sorted(parameters.items())))
    
    def get(self, source_id, parameters):
        return copy.deepcopy(self._store.get(self._make_key(source_id, parameters)))
    
    def set(self, source_id, parameters, result, ttl=None, query_id=None):
        self._store[self._make_key(source_id, parameters)] = copy.deepcopy(result)
        return True
    
    def invalidate(self, source_id, parameters=None):
        return 0
    
    def get_stats(self):
        return {"entries": len(self._store)}


def test_query_engine_applies_connector_post_processing_with_cache():
    connector = StubConnector()
    payload = {
        "metadata": {},
        "data": [{"code": "1"}],
        "schema": {"fields": [{"name": "code"}]},
    }
    
    manager = StubConnectorManager(connector, payload)
    cache = StubCacheManager()
    engine = QueryEngine(
        connector_manager=manager,
        cache_manager=cache,
        analysis_engine=DataAnalysisEngine(),
        stored_query=DummyStoredQuery(),
    )
    
    params = {"dataset": "demo"}
    first = engine.execute_query("stub_connector", params)
    assert first["data"]["data"][0]["renamed"] == "1"
    assert connector.process_calls == 1
    
    second = engine.execute_query("stub_connector", params)
    assert second["data"]["data"][0]["renamed"] == "1"
    assert connector.process_calls == 2
