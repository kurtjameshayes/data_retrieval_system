from pymongo import MongoClient
from pymongo.errors import DocumentTooLarge
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from config import Config
from bson import BSON
import hashlib
import json
import logging


logger = logging.getLogger(__name__)

class QueryResult:
    """
    Model for storing and retrieving query results from MongoDB for caching.
    """
    
    def __init__(self, db_client: MongoClient = None):
        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)
        self.db = db_client[Config.DATABASE_NAME]
        self.collection = self.db.query_results
        self.max_document_bytes = getattr(
            Config, "CACHE_MAX_DOCUMENT_BYTES", 15 * 1024 * 1024
        )
        self._create_indexes()
    
    def _create_indexes(self):
        """Create indexes for efficient querying and TTL."""
        self._ensure_index("query_hash", name="query_hash_1", unique=True)
        self._ensure_index("source_id", name="source_id_1")
        self._ensure_index(
            "expires_at",
            name="expires_at_1",
            expireAfterSeconds=0
        )

    def _ensure_index(self, keys, *, name: str, **options) -> None:
        """
        Make sure an index exists with the desired options. If an index with the
        same name exists but has different options (e.g., sparse vs non-sparse),
        drop and recreate it to avoid OperationFailure on startup.
        """
        if isinstance(keys, str):
            normalized_keys = [(keys, 1)]
        else:
            normalized_keys = list(keys)

        existing_indexes = self.collection.index_information()
        existing = existing_indexes.get(name)

        if (
            existing
            and existing.get("key") == normalized_keys
            and all(existing.get(opt) == value for opt, value in options.items())
        ):
            return

        if existing:
            logger.info("Rebuilding MongoDB index '%s' to refresh options", name)
            self.collection.drop_index(name)

        self.collection.create_index(keys, name=name, **options)
    
    def _generate_hash(self, source_id: str, parameters: Dict[str, Any]) -> str:
        """
        Generate a hash for the query to use as cache key.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            
        Returns:
            str: Hash of the query
        """
        query_string = json.dumps({
            "source_id": source_id,
            "parameters": parameters
        }, sort_keys=True)
        return hashlib.sha256(query_string.encode()).hexdigest()
    
    def save(self, source_id: str, parameters: Dict[str, Any], 
             result: Dict[str, Any], ttl: int = None, query_id: str = None) -> str:
        """
        Save query result to cache.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            result: Query result data
            ttl: Time to live in seconds (defaults to Config.CACHE_TTL)
            query_id: Optional reference to stored query
            
        Returns:
            str: Query hash
        """
        if ttl is None:
            ttl = Config.CACHE_TTL
        
        query_hash = self._generate_hash(source_id, parameters)
        expires_at = datetime.utcnow() + timedelta(seconds=ttl)
        
        cache_entry = {
            "query_hash": query_hash,
            "source_id": source_id,
            "parameters": parameters,
            "result": result,
            "created_at": datetime.utcnow(),
            "expires_at": expires_at,
            "hit_count": 0
        }
        
        # Add query_id if provided
        if query_id:
            cache_entry["query_id"] = query_id
        
        if not self._document_fits_cache(cache_entry):
            return query_hash

        try:
            self.collection.update_one(
                {"query_hash": query_hash},
                {"$set": cache_entry},
                upsert=True
            )
        except DocumentTooLarge:
            logger.warning(
                "Cache entry for %s exceeded MongoDB size limits despite pre-check; skipping",
                source_id,
            )
            return query_hash
        
        return query_hash
    
    def get(self, source_id: str, parameters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached query result.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            
        Returns:
            Dict containing cached result or None if not found/expired
        """
        query_hash = self._generate_hash(source_id, parameters)
        
        cache_entry = self.collection.find_one({
            "query_hash": query_hash,
            "expires_at": {"$gt": datetime.utcnow()}
        })
        
        if cache_entry:
            self.collection.update_one(
                {"query_hash": query_hash},
                {"$inc": {"hit_count": 1}}
            )
            cache_entry["_id"] = str(cache_entry["_id"])
            return cache_entry["result"]
        
        return None
    
    def invalidate(self, source_id: str, parameters: Dict[str, Any] = None) -> int:
        """
        Invalidate cached results.
        
        Args:
            source_id: Data source identifier
            parameters: Optional specific query parameters to invalidate
            
        Returns:
            int: Number of invalidated entries
        """
        if parameters:
            query_hash = self._generate_hash(source_id, parameters)
            result = self.collection.delete_one({"query_hash": query_hash})
            return result.deleted_count
        else:
            result = self.collection.delete_many({"source_id": source_id})
            return result.deleted_count
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict containing cache statistics
        """
        total_entries = self.collection.count_documents({})
        active_entries = self.collection.count_documents({
            "expires_at": {"$gt": datetime.utcnow()}
        })
        
        pipeline = [
            {"$group": {
                "_id": None,
                "total_hits": {"$sum": "$hit_count"}
            }}
        ]
        hit_stats = list(self.collection.aggregate(pipeline))
        total_hits = hit_stats[0]["total_hits"] if hit_stats else 0
        
        return {
            "total_entries": total_entries,
            "active_entries": active_entries,
            "expired_entries": total_entries - active_entries,
            "total_hits": total_hits
        }

    def _document_fits_cache(self, cache_entry: Dict[str, Any]) -> bool:
        """
        Determine whether a cache entry can be persisted without exceeding
        MongoDB's document size limit or the configured safety threshold.
        """
        try:
            document_size = len(BSON.encode(cache_entry))
        except Exception as exc:
            logger.warning(
                "Unable to encode cache entry for %s: %s",
                cache_entry.get("source_id"),
                exc,
            )
            return False

        if document_size > self.max_document_bytes:
            logger.info(
                "Skipping cache entry for %s – serialized size %s bytes exceeds limit %s bytes",
                cache_entry.get("source_id"),
                document_size,
                self.max_document_bytes,
            )
            return False

        return True
