#!/usr/bin/env python3
"""
Wrapper script to validate connector connections via Python.
This script is invoked by the Node.js backend via subprocess.
"""
import sys
import os
import json
import logging

os.environ["MONGO_URI"] = os.environ.get("MONGODB_URI", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.connector_manager import ConnectorManager

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 2:
        result = {
            "success": False,
            "error": "Missing source_id argument"
        }
        print(json.dumps(result))
        sys.exit(1)
    
    source_id = sys.argv[1]
    
    try:
        manager = ConnectorManager()
        connector = manager.get_connector(source_id)
        
        if not connector:
            result = {
                "success": False,
                "error": f"Connector not found or failed to load: {source_id}",
                "source_id": source_id
            }
            print(json.dumps(result))
            sys.exit(1)
        
        is_valid = connector.validate()
        
        result = {
            "success": True,
            "source_id": source_id,
            "valid": is_valid,
            "connected": connector.connected,
            "connector_type": connector.config.get("connector_type"),
            "capabilities": connector.get_capabilities()
        }
        
        print(json.dumps(result, default=str))
        
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        result = {
            "success": False,
            "error": str(e),
            "source_id": source_id
        }
        print(json.dumps(result))
        sys.exit(1)


if __name__ == "__main__":
    main()
