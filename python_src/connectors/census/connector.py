import requests
from typing import Dict, Any, List, Optional
from core.base_connector import BaseConnector
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CensusConnector(BaseConnector):
    """
    Connector for Census.gov API.
    
    API Documentation: https://www.census.gov/data/developers/guidance/api-user-guide.html
    """
    
    _variable_label_cache: Dict[str, Dict[str, str]] = {}
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("url", "https://api.census.gov/data")
        self.api_key = config.get("api_key")
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 1)
        self._current_dataset: Optional[str] = None
    
    def connect(self) -> bool:
        """Establish connection by validating API access."""
        try:
            self.connected = self.validate()
            return self.connected
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """Close connection (no persistent connection for REST API)."""
        self.connected = False
        return True
    
    def validate(self) -> bool:
        """Validate API access by making a test request."""
        try:
            test_url = f"{self.base_url}/2020/acs/acs5"
            params = {
                "get": "NAME",
                "for": "state:01"
            }
            
            if self.api_key:
                params["key"] = self.api_key
            
            response = requests.get(test_url, params=params, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return False
    
    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute query against Census.gov API.
        
        Args:
            parameters: Query parameters including:
                - dataset: Dataset identifier (e.g., "2020/acs/acs5")
                - get: Variables to retrieve (comma-separated)
                - for: Geography selection
                - in: Optional parent geography
                - additional filters
                
        Returns:
            Dict containing query results and metadata
        """
        if not self.connected:
            self.connect()
        
        dataset = parameters.get("dataset")
        if not dataset:
            raise ValueError("Dataset parameter is required")
        
        self._current_dataset = dataset
        
        query_url = f"{self.base_url}/{dataset}"
        
        query_params = {k: v for k, v in parameters.items() if k != "dataset"}
        
        if self.api_key:
            query_params["key"] = self.api_key
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    query_url,
                    params=query_params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return self.transform(data, dataset)
                elif response.status_code == 429:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"API error: {response.status_code} - {response.text}")
            
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Timeout on attempt {attempt + 1}. Retrying...")
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Error on attempt {attempt + 1}: {str(e)}. Retrying...")
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
        
        raise Exception("Max retries exceeded")
    
    def _get_variable_labels(self, dataset: str) -> Dict[str, str]:
        """
        Fetch variable labels from Census API and cache them.
        
        Args:
            dataset: Dataset identifier (e.g., "2022/acs/acs5")
            
        Returns:
            Dict mapping variable codes to their human-readable labels
        """
        if dataset in CensusConnector._variable_label_cache:
            logger.info(f"Using cached variable labels for {dataset}")
            return CensusConnector._variable_label_cache[dataset]
        
        try:
            variables_url = f"{self.base_url}/{dataset}/variables.json"
            logger.info(f"Fetching variable labels from {variables_url}")
            response = requests.get(variables_url, timeout=15)
            
            if response.status_code == 200:
                variables_data = response.json()
                variables = variables_data.get("variables", {})
                
                label_map = {}
                for var_code, var_info in variables.items():
                    if isinstance(var_info, dict):
                        label = var_info.get("label", var_code)
                        label_map[var_code] = label
                
                CensusConnector._variable_label_cache[dataset] = label_map
                logger.info(f"Cached {len(label_map)} variable labels for {dataset}")
                return label_map
            else:
                logger.warning(f"Failed to fetch variables for {dataset}: {response.status_code}")
                return {}
                
        except Exception as e:
            logger.warning(f"Error fetching variable labels for {dataset}: {str(e)}")
            return {}
    
    def transform(self, data: Any, dataset: Optional[str] = None) -> Dict[str, Any]:
        """
        Transform Census data to standardized format with human-readable column names.
        
        Census API returns data as array of arrays with first row as headers.
        Variable codes (e.g., B11001_001E) are replaced with their labels from
        the Census API variables endpoint.
        
        Args:
            data: Raw API response data
            dataset: Dataset identifier for fetching variable labels
            
        Returns:
            Dict containing standardized data with metadata
        """
        if not data or len(data) < 2:
            return {
                "metadata": self._create_metadata(0, {}),
                "data": [],
                "schema": {"fields": []}
            }
        
        raw_headers = data[0]
        
        variable_labels = {}
        if dataset:
            variable_labels = self._get_variable_labels(dataset)
        
        headers = []
        for header in raw_headers:
            if header in variable_labels:
                headers.append(variable_labels[header])
            else:
                headers.append(header)
        
        records = []
        for row in data[1:]:
            record = {}
            for i, header in enumerate(headers):
                record[header] = row[i] if i < len(row) else None
            records.append(record)
        
        standardized = {
            "metadata": self._create_metadata(len(records), {}),
            "data": records,
            "schema": {
                "fields": self._create_schema_from_headers(headers)
            }
        }
        
        return standardized
    
    def _create_schema_from_headers(self, headers: List[str]) -> List[Dict[str, str]]:
        """
        Create schema definition from headers.
        
        Args:
            headers: List of column headers
            
        Returns:
            List of field definitions
        """
        fields = []
        for header in headers:
            fields.append({
                "name": header,
                "type": "string"
            })
        return fields
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Get connector capabilities."""
        capabilities = super().get_capabilities()
        capabilities.update({
            "supports_pagination": False,
            "supports_filtering": True,
            "supports_sorting": False,
            "supports_geography": True,
            "data_formats": ["JSON"],
            "api_documentation": "https://www.census.gov/data/developers/guidance.html",
            "supports_variable_labels": True
        })
        return capabilities
    
    def get_available_datasets(self) -> List[Dict[str, Any]]:
        """
        Get list of available Census datasets.
        
        Returns:
            List of dataset information
        """
        try:
            response = requests.get(f"{self.base_url}.json", timeout=10)
            if response.status_code == 200:
                return response.json().get("dataset", [])
        except Exception as e:
            logger.error(f"Failed to retrieve datasets: {str(e)}")
        
        return []
    
    def get_dataset_variables(self, dataset: str) -> Dict[str, Any]:
        """
        Get available variables for a dataset.
        
        Args:
            dataset: Dataset identifier
            
        Returns:
            Dict of variable definitions
        """
        try:
            variables_url = f"{self.base_url}/{dataset}/variables.json"
            response = requests.get(variables_url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Failed to retrieve variables: {str(e)}")
        
        return {}
    
    def clear_variable_cache(self, dataset: Optional[str] = None):
        """
        Clear the variable label cache.
        
        Args:
            dataset: Specific dataset to clear, or None to clear all
        """
        if dataset:
            CensusConnector._variable_label_cache.pop(dataset, None)
        else:
            CensusConnector._variable_label_cache.clear()
