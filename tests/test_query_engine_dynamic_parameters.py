import pytest

from core.query_engine import QueryEngine


class InlineStoredQuery:
    def __init__(self, query_document):
        self.query_document = query_document

    def get_by_id(self, query_id):
        if self.query_document["query_id"] == query_id:
            return self.query_document
        return None


class NoopConnectorManager:
    def list_sources(self):
        return []


class NoopCacheManager:
    def get(self, *_args, **_kwargs):
        return None

    def set(self, *_args, **_kwargs):
        return True

    def get_stats(self):
        return {}


class NoopAnalysisEngine:
    def run_suite(self, *_args, **_kwargs):
        return {}


class RecordingQueryEngine(QueryEngine):
    def __init__(self, stored_query):
        super().__init__(
            connector_manager=NoopConnectorManager(),
            cache_manager=NoopCacheManager(),
            analysis_engine=NoopAnalysisEngine(),
            stored_query=stored_query,
        )
        self.last_parameters = None
        self.execute_count = 0

    def execute_query(
        self,
        source_id,
        parameters,
        use_cache=None,
        query_id=None,
        processing_context=None,
    ):
        self.last_parameters = parameters
        self.execute_count += 1
        return {"success": True, "data": {"data": []}, "source": source_id}


@pytest.fixture
def stored_query_payload():
    return {
        "query_id": "dynamic_test",
        "query_name": "Dynamic Placeholder Test",
        "connector_id": "fbi_crime",
        "parameters": {
            "endpoint": "arrest/national/all",
            "from": "{from_mm_yyyy}",
            "to": "{to_mm_yyyy}",
        },
        "active": True,
    }


def test_missing_dynamic_parameters_returns_error(stored_query_payload):
    engine = RecordingQueryEngine(InlineStoredQuery(stored_query_payload))

    result = engine.execute_stored_query("dynamic_test")

    assert result["success"] is False
    assert "Missing dynamic parameter values" in result["error"]
    assert engine.execute_count == 0


def test_parameter_name_override_resolves_placeholder(stored_query_payload):
    engine = RecordingQueryEngine(InlineStoredQuery(stored_query_payload))

    result = engine.execute_stored_query(
        "dynamic_test",
        parameter_overrides={"from": "01-2023", "to": "02-2023"},
    )

    assert result["success"] is True
    assert engine.last_parameters["from"] == "01-2023"
    assert engine.last_parameters["to"] == "02-2023"


def test_placeholder_token_override_is_supported(stored_query_payload):
    engine = RecordingQueryEngine(InlineStoredQuery(stored_query_payload))

    result = engine.execute_stored_query(
        "dynamic_test",
        parameter_overrides={"from_mm_yyyy": "03-2023", "to_mm_yyyy": "04-2023"},
    )

    assert result["success"] is True
    assert engine.last_parameters["from"] == "03-2023"
    assert engine.last_parameters["to"] == "04-2023"
