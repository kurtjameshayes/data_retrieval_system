#!/usr/bin/env python3
"""
Wrapper script to execute stored queries from the web application.
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
    if len(sys.argv) < 2:
        result = {
            "success": False,
            "error": "Missing query_id argument"
        }
        print(json.dumps(result))
        sys.exit(1)
    
    query_id = sys.argv[1]
    
    use_cache = True
    parameter_overrides = None
    
    for i, arg in enumerate(sys.argv[2:], start=2):
        if arg == "--no-cache":
            use_cache = False
        elif arg.startswith("{"):
            try:
                parameter_overrides = json.loads(arg)
            except json.JSONDecodeError:
                pass
    
    try:
        engine = QueryEngine()
        result = engine.execute_stored_query(
            query_id=query_id,
            use_cache=use_cache,
            parameter_overrides=parameter_overrides
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
            "query_id": query_id
        }
        print(json.dumps(result))
        sys.exit(1)


if __name__ == "__main__":
    main()
