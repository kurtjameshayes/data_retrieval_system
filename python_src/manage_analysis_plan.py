#!/usr/bin/env python3
"""
Wrapper script to manage analysis plans (CRUD) via Python.
This script is invoked by the Node.js backend via subprocess.
"""
import sys
import os
import json
import logging

os.environ["MONGO_URI"] = os.environ.get("MONGODB_URI", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.analysis_plan import AnalysisPlan

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 2:
        result = {
            "success": False,
            "error": "Usage: manage_analysis_plan.py <action> [args...]"
        }
        print(json.dumps(result))
        sys.exit(1)
    
    action = sys.argv[1]
    
    try:
        plan_model = AnalysisPlan()
        
        if action == "list":
            active_only = "--active" in sys.argv
            plans = plan_model.get_all(active_only=active_only)
            result = {
                "success": True,
                "plans": plans,
                "count": len(plans)
            }
            
        elif action == "get":
            if len(sys.argv) < 3:
                raise ValueError("plan_id required for get action")
            plan_id = sys.argv[2]
            plan = plan_model.get_by_id(plan_id)
            if plan:
                result = {"success": True, "plan": plan}
            else:
                result = {"success": False, "error": f"Plan not found: {plan_id}"}
                
        elif action == "create":
            if len(sys.argv) < 3:
                raise ValueError("plan_data JSON required for create action")
            plan_data = json.loads(sys.argv[2])
            created = plan_model.create(plan_data)
            result = {"success": True, "plan": created}
            
        elif action == "update":
            if len(sys.argv) < 4:
                raise ValueError("plan_id and update_data JSON required for update action")
            plan_id = sys.argv[2]
            update_data = json.loads(sys.argv[3])
            success = plan_model.update(plan_id, update_data)
            if success:
                updated = plan_model.get_by_id(plan_id)
                result = {"success": True, "plan": updated}
            else:
                result = {"success": False, "error": f"Failed to update plan: {plan_id}"}
                
        elif action == "delete":
            if len(sys.argv) < 3:
                raise ValueError("plan_id required for delete action")
            plan_id = sys.argv[2]
            success = plan_model.delete(plan_id)
            result = {"success": success, "plan_id": plan_id}
            if not success:
                result["error"] = f"Failed to delete plan: {plan_id}"
                
        elif action == "validate":
            if len(sys.argv) < 4:
                raise ValueError("plan_data JSON and columns JSON required for validate action")
            plan_data = json.loads(sys.argv[2])
            available_columns = json.loads(sys.argv[3])
            validation = plan_model.validate_columns(plan_data, available_columns)
            result = {"success": True, "validation": validation}
            
        elif action == "find_by_query":
            if len(sys.argv) < 3:
                raise ValueError("query_id required for find_by_query action")
            query_id = sys.argv[2]
            plans = plan_model.get_plans_using_query(query_id)
            result = {"success": True, "plans": plans, "count": len(plans)}
            
        else:
            result = {"success": False, "error": f"Unknown action: {action}"}
        
        print(json.dumps(result, default=str))
        
    except json.JSONDecodeError as e:
        result = {"success": False, "error": f"Invalid JSON: {str(e)}"}
        print(json.dumps(result))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        result = {"success": False, "error": str(e)}
        print(json.dumps(result))
        sys.exit(1)


if __name__ == "__main__":
    main()
