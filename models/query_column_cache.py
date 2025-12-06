from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

try:  # Python 3.11+
    from datetime import UTC
except ImportError:  # Python <3.11
    from datetime import timezone as _timezone

    UTC = _timezone.utc

from pymongo import ASCENDING, MongoClient

from config import Config

logger = logging.getLogger(__name__)


class QueryColumnCache:
    """
    Lightweight cache for storing column metadata associated with stored queries.

    Documents are automatically expired via a TTL index to ensure stale schemas
    are refreshed periodically.
    """

    COLLECTION_NAME = "query_columns"

    def __init__(
        self,
        db_client: Optional[MongoClient] = None,
        ttl_seconds: Optional[int] = None,
    ) -> None:
        self.client = db_client or MongoClient(Config.MONGO_URI)
        self.db = self.client[Config.DATABASE_NAME]
        self.collection = self.db[self.COLLECTION_NAME]
        self.ttl_seconds = ttl_seconds or Config.QUERY_COLUMNS_TTL_SECONDS
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.collection.create_index([("query_id", ASCENDING)], unique=True)
            self.collection.create_index("expires_at", expireAfterSeconds=0)
        except Exception as exc:  # pragma: no cover - defensive logging only
            logger.error("Failed to ensure query_columns indexes: %s", exc)

    def get_many(self, query_ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        """Return cached column payloads for the provided query IDs."""
        normalized = [query_id for query_id in query_ids if query_id]
        if not normalized:
            return {}

        cursor = self.collection.find(
            {
                "query_id": {"$in": normalized},
                "expires_at": {"$gt": datetime.now(UTC)},
            },
            {"_id": 0},
        )
        return {doc["query_id"]: doc for doc in cursor}

    def save(
        self,
        *,
        query_id: str,
        columns: List[str],
        connector_id: Optional[str],
        record_count: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Persist updated column metadata for a single query."""
        now = datetime.now(UTC)
        payload: Dict[str, Any] = {
            "query_id": query_id,
            "columns": list(columns),
            "column_count": len(columns),
            "connector_id": connector_id,
            "record_count": record_count,
            "updated_at": now,
            "expires_at": now + timedelta(seconds=self.ttl_seconds),
        }
        if metadata:
            payload["metadata"] = metadata

        self.collection.update_one(
            {"query_id": query_id},
            {"$set": payload},
            upsert=True,
        )

    def invalidate(self, query_id: str) -> int:
        """Remove cached metadata for a single query."""
        result = self.collection.delete_one({"query_id": query_id})
        return result.deleted_count
