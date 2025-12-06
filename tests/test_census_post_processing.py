import copy

import pandas as pd

from connectors.census.connector import CensusConnector
from core.base_connector import BaseConnector
from core.query_engine import QueryEngine
from core.data_analysis import DataAnalysisEngine


class FakeAttrRepo:
    def __init__(self, mapping):
        self.mapping = mapping
    
    def get_descriptions(self, attribute_codes):
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


class TestCensusConnectorProcessQueryResult:
    @staticmethod
    def _build_connector(mapping):
        connector = CensusConnector({"source_id": "census_api", "source_name": "Census"})
        connector._attribute_repository = FakeAttrRepo(mapping)
        return connector
    
    def test_replaces_codes(self):
        connector = self._build_connector({
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
    
    def test_without_dataset_context(self):
        connector = self._build_connector({
            "B19013_001E": "Median household income",
        })
        
        result = {
            "metadata": {},
            "data": [
                {"NAME": "ZCTA 67890", "B19013_001E": "72000"},
            ],
            "schema": {
                "fields": [
                    {"name": "NAME", "type": "string"},
                    {"name": "B19013_001E", "type": "string"},
                ]
            },
        }
        
        processed = connector.process_query_result(copy.deepcopy(result), context={})
        
        record = processed["data"][0]
        assert "Median household income" in record
        assert "B19013_001E" not in record
        
        metadata = processed["metadata"]
        assert metadata["column_name_overrides"]["B19013_001E"] == "Median household income"
        assert "dataset" not in metadata
        assert "Column names sourced from attr_name" in metadata["notes"]
    
    def test_dataframe_reflects_renamed_columns(self):
        connector = self._build_connector({
            "B01001_001E": "Total population",
        })
        
        result = {
            "metadata": {},
            "data": [
                {"NAME": "ZCTA 54321", "B01001_001E": "100"},
            ],
            "schema": {
                "fields": [
                    {"name": "NAME", "type": "string"},
                    {"name": "B01001_001E", "type": "string"},
                ]
            },
        }
        
        processed = connector.process_query_result(
            copy.deepcopy(result),
            context={"parameters": {"dataset": "2021/acs/acs5"}},
        )
        
        dataframe = pd.DataFrame(processed["data"])
        assert "Total population" in dataframe.columns
        assert "B01001_001E" not in dataframe.columns
        assert dataframe.loc[0, "Total population"] == "100"
    
    def test_duplicate_descriptions_do_not_get_mutated(self):
        connector = self._build_connector({
            "B22010_001E": "Duplicate label",
            "B22010_002E": "Duplicate label",
        })
        
        result = {
            "metadata": {},
            "data": [
                {
                    "NAME": "ZCTA 13579",
                    "B22010_001E": "10",
                    "B22010_002E": "5",
                },
            ],
            "schema": {
                "fields": [
                    {"name": "NAME", "type": "string"},
                    {"name": "B22010_001E", "type": "string"},
                    {"name": "B22010_002E", "type": "string"},
                ]
            },
        }
        
        processed = connector.process_query_result(copy.deepcopy(result), context={})
        
        record = processed["data"][0]
        assert record["Duplicate label"] == "10"
        assert record["B22010_002E"] == "5"
        assert "Duplicate label (B22010_002E)" not in record
        
        metadata = processed["metadata"]
        overrides = metadata["column_name_overrides"]
        assert overrides["B22010_001E"] == "Duplicate label"
        assert "B22010_002E" not in overrides
        assert "B22010_002E" in metadata["column_name_conflicts"]
        assert metadata["attribute_descriptions"]["B22010_002E"] == "Duplicate label"

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
