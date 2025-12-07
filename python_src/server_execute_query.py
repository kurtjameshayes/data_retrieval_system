#!/usr/bin/env python3
"""
Server-side query execution wrapper.

Outputs pure JSON for Node.js server consumption.
Supports both stored query execution (by query_id) and ad-hoc queries (source_id + params).
"""

import argparse
import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.query_engine import QueryEngine


def main():
    parser = argparse.ArgumentParser(description="Execute query and output JSON for server")
    parser.add_argument("--query-id", "-q", help="Stored query ID (MongoDB _id)")
    parser.add_argument("--source-id", "-s", help="Connector source ID for ad-hoc queries")
    parser.add_argument("--params", "-p", help="JSON parameters for ad-hoc queries")
    parser.add_argument("--no-cache", action="store_true", help="Bypass cache")
    
    args = parser.parse_args()
    
    engine = QueryEngine()
    use_cache = None if not args.no_cache else False
    
    try:
        if args.query_id:
            result = engine.execute_stored_query(args.query_id, use_cache=use_cache)
        elif args.source_id:
            params = {}
            if args.params:
                params = json.loads(args.params)
            result = engine.execute_query(args.source_id, params, use_cache=use_cache)
        else:
            result = {"success": False, "error": "Either --query-id or --source-id is required"}
    except Exception as e:
        result = {"success": False, "error": str(e)}
    
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
