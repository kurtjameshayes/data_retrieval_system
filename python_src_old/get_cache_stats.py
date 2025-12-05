#!/usr/bin/env python3
"""
Wrapper script to get cache and query statistics via Python.
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
    try:
        engine = QueryEngine()
        stats = engine.get_query_stats()
        
        result = {
            "success": True,
            "stats": stats
        }
        
        print(json.dumps(result, default=str))
        
    except Exception as e:
        logger.error(f"Stats error: {str(e)}")
        result = {
            "success": False,
            "error": str(e)
        }
        print(json.dumps(result))
        sys.exit(1)


if __name__ == "__main__":
    main()
