#!/usr/bin/env python3
"""
Wrapper script to execute analysis plans via Python.
This script is invoked by the Node.js backend via subprocess.

An analysis plan can reference multiple queries, join their results,
and run configurable analysis using DataAnalysisEngine.run_suite().
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
from models.analysis_plan import AnalysisPlan
from core.query_engine import QueryEngine
from core.data_analysis import DataAnalysisEngine

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


def execute_queries_and_join(engine: QueryEngine, queries_config: list) -> pd.DataFrame:
    """
    Execute multiple queries and join their results.
    
    Args:
        engine: QueryEngine instance
        queries_config: List of query configurations with query_id, alias, join_column
        
    Returns:
        Joined DataFrame
    """
    if not queries_config:
        raise ValueError("No queries specified in plan")
    
    dataframes = {}
    
    for i, query_cfg in enumerate(queries_config):
        query_id = query_cfg.get('query_id')
        alias = query_cfg.get('alias', f'query_{i}')
        
        if not query_id:
            raise ValueError(f"Query config at index {i} missing query_id")
        
        result = engine.execute_stored_query(query_id, use_cache=True)
        
        if not result.get('success'):
            raise ValueError(f"Query {query_id} failed: {result.get('error', 'Unknown error')}")
        
        data = result.get('data', {})
        records = data.get('data', []) if isinstance(data, dict) else data
        
        if not records:
            raise ValueError(f"Query {query_id} returned no data")
        
        df = pd.DataFrame(records)
        dataframes[alias] = {
            'df': df,
            'join_column': query_cfg.get('join_column'),
            'config': query_cfg
        }
    
    if len(dataframes) == 1:
        return list(dataframes.values())[0]['df']
    
    aliases = list(dataframes.keys())
    result_df = dataframes[aliases[0]]['df']
    
    for alias in aliases[1:]:
        df_info = dataframes[alias]
        join_col = df_info['join_column']
        
        if not join_col:
            result_df = pd.concat([result_df, df_info['df']], axis=1)
        else:
            left_join_col = dataframes[aliases[0]]['join_column'] or join_col
            if left_join_col not in result_df.columns:
                if join_col in result_df.columns:
                    left_join_col = join_col
                else:
                    for col in result_df.columns:
                        if col.lower() == join_col.lower():
                            left_join_col = col
                            break
            
            if left_join_col not in result_df.columns:
                result_df = pd.concat([result_df, df_info['df']], axis=1)
            elif join_col not in df_info['df'].columns:
                result_df = pd.concat([result_df, df_info['df']], axis=1)
            else:
                result_df = result_df.merge(
                    df_info['df'],
                    left_on=left_join_col,
                    right_on=join_col,
                    how='outer',
                    suffixes=('', f'_{alias}')
                )
    
    return result_df


def get_query_columns(engine: QueryEngine, query_id: str) -> list:
    """
    Execute a query and return its column names.
    
    Args:
        engine: QueryEngine instance
        query_id: Stored query ID
        
    Returns:
        List of column names
    """
    result = engine.execute_stored_query(query_id, use_cache=True)
    
    if not result.get('success'):
        raise ValueError(f"Query {query_id} failed: {result.get('error', 'Unknown error')}")
    
    data = result.get('data', {})
    records = data.get('data', []) if isinstance(data, dict) else data
    
    if not records:
        schema = data.get('schema', {})
        fields = schema.get('fields', [])
        if fields:
            return [f.get('name') for f in fields if f.get('name')]
        return []
    
    return list(records[0].keys())


def main():
    if len(sys.argv) < 2:
        result = {
            "success": False,
            "error": "Usage: execute_analysis_plan.py <action> [args...]"
        }
        print(json.dumps(result))
        sys.exit(1)
    
    action = sys.argv[1]
    
    try:
        plan_model = AnalysisPlan()
        engine = QueryEngine()
        analysis_engine = DataAnalysisEngine()
        
        if action == "execute":
            if len(sys.argv) < 3:
                raise ValueError("plan_id required for execute action")
            
            plan_id = sys.argv[2]
            plan = plan_model.get_by_id(plan_id)
            
            if not plan:
                raise ValueError(f"Plan not found: {plan_id}")
            
            df = execute_queries_and_join(engine, plan.get('queries', []))
            
            analysis_config = plan.get('analysis_plan', {})
            
            if not analysis_config:
                analysis_results = {
                    "basic_statistics": analysis_engine.basic_statistics(df),
                    "exploratory_analysis": analysis_engine.exploratory_analysis(df)
                }
            else:
                analysis_results = analysis_engine.run_suite(df, analysis_config)
            
            plan_model.update_run_status(plan_id, 'success')
            
            result = {
                "success": True,
                "plan_id": plan_id,
                "plan_name": plan.get('plan_name'),
                "record_count": len(df),
                "columns": list(df.columns),
                "analysis": analysis_results,
                "data_sample": df.head(10).to_dict(orient='records')
            }
            
        elif action == "get_columns":
            if len(sys.argv) < 3:
                raise ValueError("query_id required for get_columns action")
            
            query_id = sys.argv[2]
            columns = get_query_columns(engine, query_id)
            
            result = {
                "success": True,
                "query_id": query_id,
                "columns": columns
            }
            
        elif action == "get_joined_columns":
            if len(sys.argv) < 3:
                raise ValueError("queries_config JSON required for get_joined_columns action")
            
            queries_config = json.loads(sys.argv[2])
            df = execute_queries_and_join(engine, queries_config)
            
            result = {
                "success": True,
                "columns": list(df.columns),
                "record_count": len(df),
                "sample": df.head(5).to_dict(orient='records')
            }
            
        elif action == "preview":
            if len(sys.argv) < 3:
                raise ValueError("plan_id required for preview action")
            
            plan_id = sys.argv[2]
            plan = plan_model.get_by_id(plan_id)
            
            if not plan:
                raise ValueError(f"Plan not found: {plan_id}")
            
            df = execute_queries_and_join(engine, plan.get('queries', []))
            
            result = {
                "success": True,
                "plan_id": plan_id,
                "columns": list(df.columns),
                "record_count": len(df),
                "sample": df.head(20).to_dict(orient='records')
            }
            
        elif action == "validate_plan":
            if len(sys.argv) < 3:
                raise ValueError("plan_data JSON required for validate_plan action")
            
            plan_data = json.loads(sys.argv[2])
            queries_config = plan_data.get('queries', [])
            
            if queries_config:
                df = execute_queries_and_join(engine, queries_config)
                available_columns = list(df.columns)
            else:
                available_columns = []
            
            validation = plan_model.validate_columns(plan_data, available_columns)
            
            result = {
                "success": True,
                "validation": validation,
                "available_columns": available_columns
            }
            
        else:
            result = {"success": False, "error": f"Unknown action: {action}"}
        
        cleaned_result = clean_for_json(result)
        print(json.dumps(cleaned_result, default=str))
        
    except json.JSONDecodeError as e:
        result = {"success": False, "error": f"Invalid JSON: {str(e)}"}
        print(json.dumps(result))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        
        if len(sys.argv) >= 3 and sys.argv[1] == "execute":
            try:
                plan_model = AnalysisPlan()
                plan_model.update_run_status(sys.argv[2], 'error', str(e))
            except:
                pass
        
        result = {"success": False, "error": str(e)}
        print(json.dumps(result))
        sys.exit(1)


if __name__ == "__main__":
    main()
