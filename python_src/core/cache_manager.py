from typing import Dict, Any, Optional, List
from models.query_result import QueryResult
from models.query_column_cache import QueryColumnCache
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CacheManager:
    """
    Manages caching of query results and column metadata using MongoDB.
    """
    
    def __init__(self, query_result_model: QueryResult = None, 
                 column_cache_model: QueryColumnCache = None):
        """
        Initialize cache manager.
        
        Args:
            query_result_model: QueryResult model instance
            column_cache_model: QueryColumnCache model instance
        """
        self.query_result_model = query_result_model or QueryResult()
        self.column_cache = column_cache_model or QueryColumnCache()
    
    def get(self, source_id: str, parameters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached query result.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            
        Returns:
            Cached result or None if not found
        """
        try:
            result = self.query_result_model.get(source_id, parameters)
            if result:
                logger.info(f"Cache hit for {source_id}")
                return result
            else:
                logger.info(f"Cache miss for {source_id}")
                return None
        except Exception as e:
            logger.error(f"Cache retrieval error: {str(e)}")
            return None
    
    def set(self, source_id: str, parameters: Dict[str, Any], 
            result: Dict[str, Any], ttl: int = None, query_id: str = None) -> bool:
        """
        Store query result in cache.
        
        Args:
            source_id: Data source identifier
            parameters: Query parameters
            result: Query result to cache
            ttl: Time to live in seconds
            query_id: Optional reference to stored query
            
        Returns:
            bool: True if successful
        """
        try:
            self.query_result_model.save(source_id, parameters, result, ttl, query_id)
            logger.info(f"Cached result for {source_id}")
            return True
        except Exception as e:
            logger.error(f"Cache storage error: {str(e)}")
            return False
    
    def invalidate(self, source_id: str, parameters: Dict[str, Any] = None) -> int:
        """
        Invalidate cached results.
        
        Args:
            source_id: Data source identifier
            parameters: Optional specific query to invalidate
            
        Returns:
            Number of invalidated entries
        """
        try:
            count = self.query_result_model.invalidate(source_id, parameters)
            logger.info(f"Invalidated {count} cache entries for {source_id}")
            return count
        except Exception as e:
            logger.error(f"Cache invalidation error: {str(e)}")
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict containing cache statistics
        """
        try:
            return self.query_result_model.get_stats()
        except Exception as e:
            logger.error(f"Failed to get cache stats: {str(e)}")
            return {
                "error": str(e)
            }
    
    def cache_query_columns(
        self, 
        query_id: str, 
        columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
        row_count: Optional[int] = None
    ) -> bool:
        """
        Cache column names for a query.
        
        Args:
            query_id: Stored query identifier
            columns: List of column names
            column_types: Optional dict mapping column names to types
            row_count: Optional row count from query results
            
        Returns:
            bool: True if successful
        """
        return self.column_cache.cache_columns(query_id, columns, column_types, row_count)
    
    def get_query_columns(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Get cached columns for a query.
        
        Args:
            query_id: Stored query identifier
            
        Returns:
            Dict with columns, column_types, row_count or None if not found
        """
        return self.column_cache.get_columns(query_id)
    
    def get_columns_for_queries(self, query_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get cached columns for multiple queries.
        
        Args:
            query_ids: List of stored query identifiers
            
        Returns:
            Dict mapping query_id to column info
        """
        return self.column_cache.get_columns_for_queries(query_ids)
    
    def invalidate_query_columns(self, query_id: str) -> bool:
        """
        Invalidate cached columns for a query.
        
        Args:
            query_id: Stored query identifier
            
        Returns:
            bool: True if entry was deleted
        """
        return self.column_cache.invalidate(query_id)
