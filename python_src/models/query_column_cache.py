"""
Query Column Cache Model

Caches column names for stored queries in MongoDB to enable fast column selection
in Analysis Plans without needing to execute queries.
"""

from pymongo import MongoClient, ASCENDING
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from config import Config
import logging

logger = logging.getLogger(__name__)


class QueryColumnCache:
    """
    Model for caching query column names in MongoDB.
    
    Schema:
        query_id: Reference to stored query
        columns: List of column names returned by the query
        column_types: Optional dict mapping column names to data types
        row_count: Number of rows returned (for reference)
        cached_at: When the columns were cached
        expires_at: When the cache entry expires
    """
    
    def __init__(self, db_client: MongoClient = None):
        """Initialize QueryColumnCache model."""
        if db_client is None:
            db_client = MongoClient(Config.MONGO_URI)
        self.db = db_client[Config.DATABASE_NAME]
        self.collection = self.db['query_column_cache']
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create indexes for the query_column_cache collection."""
        try:
            self.collection.create_index([("query_id", ASCENDING)], unique=True)
            self.collection.create_index("expires_at", expireAfterSeconds=0)
            logger.info("QueryColumnCache indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating QueryColumnCache indexes: {str(e)}")
    
    def cache_columns(
        self, 
        query_id: str, 
        columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
        row_count: Optional[int] = None,
        ttl_seconds: int = 86400 * 7
    ) -> bool:
        """
        Cache column names for a query.
        
        Args:
            query_id: Stored query identifier
            columns: List of column names
            column_types: Optional dict mapping column names to types
            row_count: Optional row count from query results
            ttl_seconds: Time to live in seconds (default 7 days)
            
        Returns:
            bool: True if successful
        """
        try:
            now = datetime.utcnow()
            cache_entry = {
                "query_id": query_id,
                "columns": columns,
                "column_types": column_types or {},
                "row_count": row_count,
                "cached_at": now,
                "expires_at": now + timedelta(seconds=ttl_seconds)
            }
            
            self.collection.update_one(
                {"query_id": query_id},
                {"$set": cache_entry},
                upsert=True
            )
            
            logger.info(f"Cached {len(columns)} columns for query: {query_id}")
            return True
        except Exception as e:
            logger.error(f"Error caching columns for {query_id}: {str(e)}")
            return False
    
    def get_columns(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Get cached columns for a query.
        
        Args:
            query_id: Stored query identifier
            
        Returns:
            Dict with columns, column_types, row_count or None if not found/expired
        """
        try:
            cache_entry = self.collection.find_one({
                "query_id": query_id,
                "expires_at": {"$gt": datetime.utcnow()}
            })
            
            if cache_entry:
                return {
                    "query_id": cache_entry["query_id"],
                    "columns": cache_entry["columns"],
                    "column_types": cache_entry.get("column_types", {}),
                    "row_count": cache_entry.get("row_count"),
                    "cached_at": cache_entry["cached_at"].isoformat() if cache_entry.get("cached_at") else None
                }
            return None
        except Exception as e:
            logger.error(f"Error getting cached columns for {query_id}: {str(e)}")
            return None
    
    def get_columns_for_queries(self, query_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get cached columns for multiple queries.
        
        Args:
            query_ids: List of stored query identifiers
            
        Returns:
            Dict mapping query_id to column info
        """
        try:
            result = {}
            cache_entries = self.collection.find({
                "query_id": {"$in": query_ids},
                "expires_at": {"$gt": datetime.utcnow()}
            })
            
            for entry in cache_entries:
                result[entry["query_id"]] = {
                    "columns": entry["columns"],
                    "column_types": entry.get("column_types", {}),
                    "row_count": entry.get("row_count"),
                    "cached_at": entry["cached_at"].isoformat() if entry.get("cached_at") else None
                }
            
            return result
        except Exception as e:
            logger.error(f"Error getting cached columns for queries: {str(e)}")
            return {}
    
    def invalidate(self, query_id: str) -> bool:
        """
        Invalidate cached columns for a query.
        
        Args:
            query_id: Stored query identifier
            
        Returns:
            bool: True if entry was deleted
        """
        try:
            result = self.collection.delete_one({"query_id": query_id})
            if result.deleted_count > 0:
                logger.info(f"Invalidated column cache for query: {query_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error invalidating column cache for {query_id}: {str(e)}")
            return False
    
    def invalidate_all(self) -> int:
        """
        Invalidate all cached columns.
        
        Returns:
            int: Number of entries deleted
        """
        try:
            result = self.collection.delete_many({})
            logger.info(f"Invalidated {result.deleted_count} column cache entries")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error invalidating all column caches: {str(e)}")
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict containing cache statistics
        """
        try:
            total_entries = self.collection.count_documents({})
            active_entries = self.collection.count_documents({
                "expires_at": {"$gt": datetime.utcnow()}
            })
            
            return {
                "total_entries": total_entries,
                "active_entries": active_entries,
                "expired_entries": total_entries - active_entries
            }
        except Exception as e:
            logger.error(f"Failed to get column cache stats: {str(e)}")
            return {"error": str(e)}
