#!/usr/bin/env python3
"""
JSON wrapper for stored query execution.
Calls the same QueryEngine as execute_query.py but outputs pure JSON for server consumption.
"""

import sys
import os
import json
import argparse

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.query_engine import QueryEngine


def main():
    parser = argparse.ArgumentParser(description="Execute stored query and output JSON")
    parser.add_argument("query_id", help="ID of the stored query to execute")
    parser.add_argument("--no-cache", action="store_true", help="Bypass cache")
    
    args = parser.parse_args()
    
    engine = QueryEngine()
    use_cache = None if not args.no_cache else False
    
    result = engine.execute_stored_query(args.query_id, use_cache=use_cache)
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
