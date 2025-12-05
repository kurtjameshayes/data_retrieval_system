#!/usr/bin/env python3
"""
Wrapper script to execute ad-hoc queries (not stored queries) via Python.
This script is invoked by the Node.js backend via subprocess.
"""
import sys
import os
import json
import logging

os.environ["MONGO_URI"] = os.environ.get("MONGODB_URI", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.query_engine import QueryEngine

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 3:
        result = {
            "success": False,
            "error": "Usage: execute_query.py <source_id> <parameters_json> [--no-cache]"
        }
        print(json.dumps(result))
        sys.exit(1)
    
    source_id = sys.argv[1]
    
    try:
        parameters = json.loads(sys.argv[2])
    except json.JSONDecodeError as e:
        result = {
            "success": False,
            "error": f"Invalid parameters JSON: {str(e)}"
        }
        print(json.dumps(result))
        sys.exit(1)
    
    use_cache = "--no-cache" not in sys.argv
    
    try:
        engine = QueryEngine()
        result = engine.execute_query(
            source_id=source_id,
            parameters=parameters,
            use_cache=use_cache
        )
        
        if "data" in result and isinstance(result["data"], dict):
            data = result["data"]
            if "data" in data and isinstance(data["data"], list):
                result["record_count"] = len(data["data"])
        
        print(json.dumps(result, default=str))
        
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        result = {
            "success": False,
            "error": str(e),
            "source_id": source_id
        }
        print(json.dumps(result))
        sys.exit(1)


if __name__ == "__main__":
    main()
