"""
Analysis Plan Model

Manages analysis plans in MongoDB for configurable multi-query joins and ML-based analysis.
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from config import Config
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class AnalysisPlan:
    """
    Model for managing analysis plans in MongoDB.
    
    Schema:
        plan_id: Unique identifier for the plan
        plan_name: Human-readable name
        description: Optional description of what the analysis does
        
        # Query configuration for data joining
        queries: List of query configurations for joining
            - query_id: Reference to stored_query
            - alias: Short name for this query in the analysis
            - join_column: Column to use for joining with other queries
            
        # Analysis configuration 
        analysis_plan: Configuration for DataAnalysisEngine.run_suite()
            - basic_statistics: bool
            - exploratory: bool
            - inferential_tests: List of test configurations
            - time_series: Time series config with time_column, target_column
            - linear_regression: Regression config with features, target
            - random_forest: Random forest config with features, target
            - multivariate: PCA config with features, n_components
            - predictive: Predictive model config
            
        created_at: Creation timestamp
        updated_at: Last update timestamp
        tags: Optional list of tags
        active: Whether the plan is active
        last_run_at: Timestamp of last execution
        last_run_status: Success/error status of last run
    """
    
    def __init__(self):
        """Initialize AnalysisPlan model."""
        self.client = MongoClient(Config.MONGO_URI)
        self.db = self.client[Config.DATABASE_NAME]
        self.collection = self.db['analysis_plans']
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create indexes for the analysis_plans collection."""
        try:
            self.collection.create_index([("plan_id", ASCENDING)], unique=True)
            self.collection.create_index([("tags", ASCENDING)])
            self.collection.create_index([("active", ASCENDING)])
            self.collection.create_index([("created_at", DESCENDING)])
            
            logger.info("AnalysisPlan indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating AnalysisPlan indexes: {str(e)}")
    
    def create(self, plan_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new analysis plan.
        
        Args:
            plan_data: Dictionary containing plan information
                Required fields: plan_id, plan_name, queries, analysis_plan
                Optional fields: description, tags
        
        Returns:
            dict: Created plan document
        """
        required_fields = ['plan_id', 'plan_name', 'queries', 'analysis_plan']
        for field in required_fields:
            if field not in plan_data:
                raise ValueError(f"Missing required field: {field}")
        
        if not plan_data['queries'] or len(plan_data['queries']) == 0:
            raise ValueError("At least one query is required")
        
        now = datetime.utcnow()
        plan_data['created_at'] = now
        plan_data['updated_at'] = now
        
        if 'active' not in plan_data:
            plan_data['active'] = True
        
        if 'tags' not in plan_data:
            plan_data['tags'] = []
        
        plan_data['last_run_at'] = None
        plan_data['last_run_status'] = None
        
        try:
            self.collection.insert_one(plan_data)
            logger.info(f"Created analysis plan: {plan_data['plan_id']}")
            plan_data.pop('_id', None)
            return plan_data
        except Exception as e:
            logger.error(f"Error creating analysis plan: {str(e)}")
            raise
    
    def get_by_id(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an analysis plan by ID.
        
        Args:
            plan_id: Plan identifier
            
        Returns:
            dict: Plan document or None if not found
        """
        try:
            plan = self.collection.find_one({"plan_id": plan_id})
            if plan:
                plan.pop('_id', None)
            return plan
        except Exception as e:
            logger.error(f"Error getting plan {plan_id}: {str(e)}")
            return None
    
    def get_all(self, 
                active_only: bool = False,
                tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get all analysis plans with optional filtering.
        
        Args:
            active_only: Only return active plans
            tags: Filter by tags
            
        Returns:
            list: List of plan documents
        """
        try:
            filter_dict = {}
            
            if active_only:
                filter_dict['active'] = True
            
            if tags:
                filter_dict['tags'] = {'$in': tags}
            
            plans = list(self.collection.find(filter_dict).sort("plan_name", ASCENDING))
            
            for plan in plans:
                plan.pop('_id', None)
            
            return plans
        except Exception as e:
            logger.error(f"Error getting plans: {str(e)}")
            return []
    
    def update(self, plan_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update an analysis plan.
        
        Args:
            plan_id: Plan identifier
            update_data: Dictionary of fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data['updated_at'] = datetime.utcnow()
            update_data.pop('plan_id', None)
            
            result = self.collection.update_one(
                {"plan_id": plan_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated analysis plan: {plan_id}")
                return True
            else:
                logger.warning(f"No plan found to update: {plan_id}")
                return False
        except Exception as e:
            logger.error(f"Error updating plan {plan_id}: {str(e)}")
            return False
    
    def delete(self, plan_id: str) -> bool:
        """
        Delete an analysis plan.
        
        Args:
            plan_id: Plan identifier
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"plan_id": plan_id})
            
            if result.deleted_count > 0:
                logger.info(f"Deleted analysis plan: {plan_id}")
                return True
            else:
                logger.warning(f"No plan found to delete: {plan_id}")
                return False
        except Exception as e:
            logger.error(f"Error deleting plan {plan_id}: {str(e)}")
            return False
    
    def update_run_status(self, plan_id: str, status: str, error: Optional[str] = None) -> bool:
        """
        Update the last run status of a plan.
        
        Args:
            plan_id: Plan identifier
            status: 'success' or 'error'
            error: Error message if status is 'error'
            
        Returns:
            bool: True if successful
        """
        try:
            update_data = {
                'last_run_at': datetime.utcnow(),
                'last_run_status': status,
                'updated_at': datetime.utcnow()
            }
            
            if error:
                update_data['last_run_error'] = error
            
            result = self.collection.update_one(
                {"plan_id": plan_id},
                {"$set": update_data}
            )
            
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating run status for {plan_id}: {str(e)}")
            return False
    
    def get_plans_using_query(self, query_id: str) -> List[Dict[str, Any]]:
        """
        Find all plans that use a specific query.
        
        Args:
            query_id: Query identifier
            
        Returns:
            list: List of plans using this query
        """
        try:
            plans = list(self.collection.find({
                "queries.query_id": query_id
            }))
            
            for plan in plans:
                plan.pop('_id', None)
            
            return plans
        except Exception as e:
            logger.error(f"Error finding plans for query {query_id}: {str(e)}")
            return []
    
    def validate_columns(self, plan_data: Dict[str, Any], available_columns: List[str]) -> Dict[str, Any]:
        """
        Validate that all referenced columns exist in available_columns.
        
        Args:
            plan_data: Plan configuration
            available_columns: List of columns available from joined queries
            
        Returns:
            dict: Validation result with 'valid' bool and 'errors' list
        """
        errors = []
        config = plan_data.get('analysis_plan', {})
        
        for query_cfg in plan_data.get('queries', []):
            join_col = query_cfg.get('join_column')
            if join_col and join_col not in available_columns:
                errors.append(f"Join column '{join_col}' not found in query output")
        
        if config.get('time_series'):
            ts_cfg = config['time_series']
            if ts_cfg.get('time_column') and ts_cfg['time_column'] not in available_columns:
                errors.append(f"Time column '{ts_cfg['time_column']}' not found")
            if ts_cfg.get('target_column') and ts_cfg['target_column'] not in available_columns:
                errors.append(f"Target column '{ts_cfg['target_column']}' not found")
        
        for analysis_type in ['linear_regression', 'random_forest', 'predictive']:
            if config.get(analysis_type):
                cfg = config[analysis_type]
                if cfg.get('target') and cfg['target'] not in available_columns:
                    errors.append(f"{analysis_type} target '{cfg['target']}' not found")
                for feature in cfg.get('features', []):
                    if feature not in available_columns:
                        errors.append(f"{analysis_type} feature '{feature}' not found")
        
        if config.get('multivariate'):
            for feature in config['multivariate'].get('features', []):
                if feature not in available_columns:
                    errors.append(f"Multivariate feature '{feature}' not found")
        
        if config.get('inferential_tests'):
            for test in config['inferential_tests']:
                if test.get('x') and test['x'] not in available_columns:
                    errors.append(f"Inferential test x column '{test['x']}' not found")
                if test.get('y') and test['y'] not in available_columns:
                    errors.append(f"Inferential test y column '{test['y']}' not found")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def count(self, active_only: bool = False) -> int:
        """
        Count analysis plans.
        
        Args:
            active_only: Only count active plans
            
        Returns:
            int: Number of plans
        """
        try:
            filter_dict = {}
            if active_only:
                filter_dict['active'] = True
            return self.collection.count_documents(filter_dict)
        except Exception as e:
            logger.error(f"Error counting plans: {str(e)}")
            return 0
