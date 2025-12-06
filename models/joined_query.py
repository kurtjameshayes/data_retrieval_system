from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from bson import BSON
from pymongo import ASCENDING, DESCENDING, MongoClient

from config import Config

logger = logging.getLogger(__name__)


class JoinedQueryStore:
    """Persist joined DataFrame outputs for executed analysis plans."""

    COLLECTION_NAME = "joined_queries"

    def __init__(
        self,
        db_client: Optional[MongoClient] = None,
        max_document_bytes: Optional[int] = None,
    ) -> None:
        self.client = db_client or MongoClient(Config.MONGO_URI)
        self.db = self.client[Config.DATABASE_NAME]
        self.collection = self.db[self.COLLECTION_NAME]
        self.max_document_bytes = (
            max_document_bytes or Config.JOINED_QUERY_MAX_DOCUMENT_BYTES
        )
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.collection.create_index([("plan_id", ASCENDING)])
            self.collection.create_index([("executed_at", DESCENDING)])
        except Exception as exc:  # pragma: no cover - defensive logging only
            logger.error("Failed to ensure joined_queries indexes: %s", exc)

    def save_execution(self, document: Dict[str, Any]) -> Optional[str]:
        """
        Persist a joined query document.

        Returns the inserted document ID as a string when saved successfully.
        """
        plan_id = document.get("plan_id")
        if not plan_id:
            raise ValueError("plan_id is required to persist joined query data")

        if not document.get("data"):
            logger.info(
                "Joined query payload for plan '%s' contains no data; skipping persistence",
                plan_id,
            )
            return None

        if not self._document_fits(document):
            logger.warning(
                "Joined query payload for plan '%s' exceeded %s bytes; skipping persistence",
                plan_id,
                self.max_document_bytes,
            )
            return None

        result = self.collection.insert_one(document)
        return str(result.inserted_id)

    def _document_fits(self, document: Dict[str, Any]) -> bool:
        """Ensure the Mongo document does not exceed configured limits."""
        try:
            encoded = BSON.encode(document)
        except Exception as exc:  # pragma: no cover - defensive logging only
            logger.warning(
                "Unable to encode joined query document for plan '%s': %s",
                document.get("plan_id"),
                exc,
            )
            return False

        return len(encoded) <= self.max_document_bytes
