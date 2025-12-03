from unittest.mock import MagicMock

from models.query_result import QueryResult


def _build_query_result():
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()

    mock_client.__getitem__.return_value = mock_db
    mock_db.query_results = mock_collection

    query_result = QueryResult(db_client=mock_client)
    # Guard tests from interacting with a real Mongo instance
    query_result.collection = mock_collection
    return query_result, mock_collection


def test_cache_skip_when_document_exceeds_limit():
    query_result, mock_collection = _build_query_result()
    query_result.max_document_bytes = 64

    large_payload = {"data": "x" * 2048}
    query_hash = query_result.save("test-source", {"param": 1}, large_payload)

    assert query_hash is not None
    mock_collection.update_one.assert_not_called()


def test_cache_persists_when_document_within_limit():
    query_result, mock_collection = _build_query_result()
    query_result.max_document_bytes = 1024 * 1024

    small_payload = {"data": "value"}
    query_hash = query_result.save("test-source", {"param": 1}, small_payload)

    assert query_hash is not None
    mock_collection.update_one.assert_called_once()
