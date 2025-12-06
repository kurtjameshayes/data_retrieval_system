#!/usr/bin/env python3
"""
Get Query Columns Script

Retrieves cached column information for stored queries.
If columns are not cached, executes the query to get them.
"""

import sys
import os
import json
import argparse
import logging

os.environ["MONGO_URI"] = os.environ.get("MONGODB_URI", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

from core.query_engine import QueryEngine
from models.query_column_cache import QueryColumnCache


def get_columns_for_query(query_id: str, force_refresh: bool = False):
    """
    Get cached columns for a query, optionally refreshing by executing the query.
    
    Args:
        query_id: Stored query identifier
        force_refresh: If True, execute the query to refresh column cache
        
    Returns:
        Dict with columns info
    """
    engine = QueryEngine()
    column_cache = QueryColumnCache()
    
    if not force_refresh:
        cached = column_cache.get_columns(query_id)
        if cached:
            return {
                "success": True,
                "source": "cache",
                **cached
            }
    
    result = engine.execute_stored_query(query_id, use_cache=True)
    
    if not result.get("success"):
        return {
            "success": False,
            "error": result.get("error", "Query execution failed"),
            "query_id": query_id
        }
    
    cached = column_cache.get_columns(query_id)
    if cached:
        return {
            "success": True,
            "source": "executed",
            **cached
        }
    
    records = engine._extract_records(result)
    if records:
        columns = list(records[0].keys())
        column_types = {col: type(val).__name__ for col, val in records[0].items()}
        return {
            "success": True,
            "source": "executed",
            "query_id": query_id,
            "columns": columns,
            "column_types": column_types,
            "row_count": len(records)
        }
    
    return {
        "success": False,
        "error": "No data returned from query",
        "query_id": query_id
    }


def get_columns_for_queries(query_ids: list, force_refresh: bool = False):
    """
    Get cached columns for multiple queries.
    
    Args:
        query_ids: List of stored query identifiers
        force_refresh: If True, execute queries to refresh column cache
        
    Returns:
        Dict mapping query_id to column info
    """
    column_cache = QueryColumnCache()
    
    if not force_refresh:
        cached = column_cache.get_columns_for_queries(query_ids)
        missing_ids = [qid for qid in query_ids if qid not in cached]
        
        if not missing_ids:
            return {
                "success": True,
                "source": "cache",
                "columns": cached
            }
    else:
        missing_ids = query_ids
        cached = {}
    
    engine = QueryEngine()
    for query_id in missing_ids:
        result = engine.execute_stored_query(query_id, use_cache=True)
        if result.get("success"):
            records = engine._extract_records(result)
            if records:
                columns = list(records[0].keys())
                column_types = {col: type(val).__name__ for col, val in records[0].items()}
                cached[query_id] = {
                    "columns": columns,
                    "column_types": column_types,
                    "row_count": len(records)
                }
    
    return {
        "success": True,
        "source": "mixed" if not force_refresh else "executed",
        "columns": cached
    }


def main():
    parser = argparse.ArgumentParser(description="Get cached columns for stored queries")
    parser.add_argument("--query-id", "-q", help="Single query ID to get columns for")
    parser.add_argument("--query-ids", "-Q", help="JSON array of query IDs")
    parser.add_argument("--force-refresh", "-f", action="store_true",
                        help="Force refresh by executing queries")
    
    args = parser.parse_args()
    
    if args.query_id:
        result = get_columns_for_query(args.query_id, args.force_refresh)
    elif args.query_ids:
        query_ids = json.loads(args.query_ids)
        result = get_columns_for_queries(query_ids, args.force_refresh)
    else:
        result = {"success": False, "error": "No query ID(s) provided"}
    
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
