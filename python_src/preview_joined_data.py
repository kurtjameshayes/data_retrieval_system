#!/usr/bin/env python3
"""
Preview Joined Data Script

Executes queries from an analysis plan and joins the results 
without running analysis - just returns the joined data preview.
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
from models.analysis_plan import AnalysisPlan
from models.stored_query import StoredQuery


def preview_joined_data(plan_id: str = None, plan_data: dict = None, limit: int = 100, join_type: str = None):
    """
    Execute queries and return joined data preview without analysis.
    
    Args:
        plan_id: Analysis plan ID to load from MongoDB
        plan_data: Analysis plan data (alternative to plan_id)
        limit: Maximum number of rows to return
        join_type: Override join type (inner, left, right, outer)
        
    Returns:
        Dict with joined data preview
    """
    if plan_id:
        plan_model = AnalysisPlan()
        plan = plan_model.get_by_id(plan_id)
        if not plan:
            return {
                "success": False,
                "error": f"Analysis plan not found: {plan_id}"
            }
    elif plan_data:
        plan = plan_data
    else:
        return {
            "success": False,
            "error": "Either plan_id or plan_data is required"
        }
    
    query_configs = plan.get("queries", [])
    
    if len(query_configs) < 1:
        return {
            "success": False,
            "error": "At least 1 query is required"
        }
    
    how = join_type if join_type else plan.get("join_type", "inner")
    if how not in ("inner", "left", "right", "outer"):
        how = "inner"
    
    engine = QueryEngine()
    stored_query = StoredQuery()
    
    queries = []
    for qc in query_configs:
        query_id = qc.get("query_id")
        sq = stored_query.get_by_id(query_id)
        if not sq:
            return {
                "success": False,
                "error": f"Stored query not found: {query_id}"
            }
        query_spec = {
            "source_id": sq["connector_id"],
            "parameters": sq["parameters"],
            "alias": qc.get("alias", query_id)
        }
        if qc.get("join_column"):
            query_spec["join_column"] = qc["join_column"]
        if qc.get("join_columns"):
            query_spec["join_columns"] = qc["join_columns"]
        queries.append(query_spec)
    
    try:
        import pandas as pd
        
        if len(queries) == 1:
            result = engine.execute_stored_query(query_configs[0]["query_id"], use_cache=True)
            if not result.get("success"):
                return {
                    "success": False,
                    "error": result.get("error", "Query execution failed")
                }
            records = engine._extract_records(result)
            if not records:
                return {
                    "success": False,
                    "error": "No data returned from query"
                }
            
            df = pd.DataFrame(records)
        else:
            df = engine.execute_queries_to_dataframe(
                queries=queries,
                how=how
            )
        
        total_rows = len(df)
        total_columns = len(df.columns)
        columns = list(df.columns)
        column_types = {col: str(df[col].dtype) for col in df.columns}
        
        preview_df = df.head(limit)
        rows = preview_df.to_dict(orient="records")
        
        for row in rows:
            for key, value in row.items():
                if hasattr(value, 'item'):
                    row[key] = value.item()
                elif str(value) == 'nan' or str(value) == 'NaN':
                    row[key] = None
        
        return {
            "success": True,
            "plan_id": plan_id,
            "total_rows": total_rows,
            "total_columns": total_columns,
            "preview_rows": len(rows),
            "columns": columns,
            "column_types": column_types,
            "data": rows
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def main():
    parser = argparse.ArgumentParser(description="Preview joined query data")
    parser.add_argument("--plan-id", "-p", help="Analysis plan ID")
    parser.add_argument("--join-type", "-t", default="inner",
                        choices=["inner", "left", "right", "outer"],
                        help="Join type")
    parser.add_argument("--limit", "-l", type=int, default=100,
                        help="Maximum number of rows to return")
    
    args = parser.parse_args()
    
    if args.plan_id:
        result = preview_joined_data(plan_id=args.plan_id, limit=args.limit, join_type=args.join_type)
    else:
        result = {"success": False, "error": "--plan-id is required"}
    
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
