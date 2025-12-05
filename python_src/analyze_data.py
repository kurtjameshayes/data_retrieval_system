#!/usr/bin/env python3
"""
Wrapper script to run data analysis on query results via Python.
This script is invoked by the Node.js backend via subprocess.
"""
import sys
import os
import json
import logging
import math

os.environ["MONGO_URI"] = os.environ.get("MONGODB_URI", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from core.data_analysis import DataAnalysisEngine
from core.query_engine import QueryEngine

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def clean_for_json(obj):
    """Recursively clean an object for JSON serialization by replacing NaN/Inf with None."""
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (np.floating, np.integer)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj) if isinstance(obj, np.floating) else int(obj)
    elif pd.isna(obj):
        return None
    return obj


def main():
    if len(sys.argv) < 2:
        result = {
            "success": False,
            "error": "Usage: analyze_data.py <query_id> [analysis_type]"
        }
        print(json.dumps(result))
        sys.exit(1)
    
    query_id = sys.argv[1]
    analysis_type = sys.argv[2] if len(sys.argv) > 2 else "basic"
    
    try:
        engine = QueryEngine()
        analysis_engine = DataAnalysisEngine()
        
        query_result = engine.execute_stored_query(query_id, use_cache=True)
        
        if not query_result.get("success"):
            print(json.dumps(query_result))
            sys.exit(1)
        
        data = query_result.get("data", {})
        records = data.get("data", []) if isinstance(data, dict) else data
        
        if not records:
            result = {
                "success": False,
                "error": "No data available for analysis",
                "query_id": query_id
            }
            print(json.dumps(result))
            sys.exit(1)
        
        df = pd.DataFrame(records)
        
        analysis_result = {
            "success": True,
            "query_id": query_id,
            "record_count": len(records),
            "analysis_type": analysis_type
        }
        
        if analysis_type == "basic":
            analysis_result["analysis"] = analysis_engine.basic_statistics(df)
        elif analysis_type == "exploratory":
            analysis_result["analysis"] = analysis_engine.exploratory_analysis(df)
        elif analysis_type == "full":
            analysis_result["analysis"] = {
                "basic": analysis_engine.basic_statistics(df),
                "exploratory": analysis_engine.exploratory_analysis(df)
            }
        else:
            result = {
                "success": False,
                "error": f"Unknown analysis type: {analysis_type}. Use 'basic', 'exploratory', or 'full'"
            }
            print(json.dumps(result))
            sys.exit(1)
        
        cleaned_result = clean_for_json(analysis_result)
        print(json.dumps(cleaned_result, default=str))
        
    except Exception as e:
        logger.error(f"Analysis error: {str(e)}")
        result = {
            "success": False,
            "error": str(e),
            "query_id": query_id
        }
        print(json.dumps(result))
        sys.exit(1)


if __name__ == "__main__":
    main()
