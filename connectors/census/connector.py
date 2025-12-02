import logging
import time
from typing import Dict, Any, List, Optional

import requests
from pymongo import MongoClient

from config import Config
from core.base_connector import BaseConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CensusConnector(BaseConnector):
    """
    Connector for Census.gov API.
    
    API Documentation: https://www.census.gov/data/developers/guidance/api-user-guide.html
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("url", "https://api.census.gov/data")
        self.api_key = config.get("api_key")  # Optional but recommended
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 1)
        self._attribute_repository = CensusAttributeNameRepository()
    
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
            # Test with a simple request to the 2020 ACS 5-year data
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
        
        # Extract dataset from parameters
        dataset = parameters.get("dataset")
        if not dataset:
            raise ValueError("Dataset parameter is required")
        
        # Build query URL
        query_url = f"{self.base_url}/{dataset}"
        
        # Build query parameters
        query_params = {k: v for k, v in parameters.items() if k != "dataset"}
        
        if self.api_key:
            query_params["key"] = self.api_key
        
        # Execute query with retry logic
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    query_url,
                    params=query_params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return self.transform(data)
                elif response.status_code == 429:  # Rate limit
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
    
    def transform(self, data: Any) -> Dict[str, Any]:
        """
        Transform Census data to standardized format.
        
        Census API returns data as array of arrays with first row as headers.
        
        Args:
            data: Raw API response data
            
        Returns:
            Dict containing standardized data with metadata
        """
        if not data or len(data) < 2:
            return {
                "metadata": self._create_metadata(0, {}),
                "data": [],
                "schema": {"fields": []}
            }
        
        # First row contains headers
        headers = data[0]
        
        # Convert remaining rows to dictionaries
        records = []
        for row in data[1:]:
            record = {}
            for i, header in enumerate(headers):
                record[header] = row[i] if i < len(row) else None
            records.append(record)
        
        # Create standardized response
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
                "type": "string"  # Census API returns all as strings
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
            "api_documentation": "https://www.census.gov/data/developers/guidance.html"
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
    
    def process_query_result(self, result: Dict[str, Any],
                             context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Replace Census variable codes with human-friendly descriptions sourced
        from the attr_name collection when available.
        """
        if not result:
            return result
        
        dataset = self._extract_dataset_from_context(context)
        if not dataset:
            return result
        
        column_order = self._collect_column_names(result)
        if not column_order:
            return result
        
        description_map = self._attribute_repository.get_descriptions(dataset, column_order)
        if not description_map:
            return result
        
        rename_map = self._build_final_name_map(column_order, description_map)
        if not rename_map:
            return result
        
        self._rename_records(result.get("data"), rename_map)
        self._rename_schema(result.get("schema"), rename_map)
        self._record_metadata_overrides(result, rename_map, description_map, dataset)
        return result
    
    def _extract_dataset_from_context(self, context: Optional[Dict[str, Any]]) -> Optional[str]:
        if not context:
            return None
        
        parameters = context.get("parameters") or {}
        dataset = parameters.get("dataset")
        if dataset:
            return dataset
        
        stored_query = context.get("stored_query") or {}
        stored_params = stored_query.get("parameters") or {}
        return stored_params.get("dataset")
    
    def _collect_column_names(self, result: Dict[str, Any]) -> List[str]:
        schema = result.get("schema", {})
        fields = schema.get("fields", []) if isinstance(schema, dict) else []
        columns = [field.get("name") for field in fields if isinstance(field, dict) and field.get("name")]
        
        if not columns:
            records = result.get("data") or []
            if records and isinstance(records, list):
                first_record = next((r for r in records if isinstance(r, dict)), {})
                columns = list(first_record.keys())
        
        return columns
    
    def _build_final_name_map(self, column_order: List[str], description_map: Dict[str, str]) -> Dict[str, str]:
        rename_candidates = [name for name in column_order if name in description_map]
        if not rename_candidates:
            return {}
        
        stable_columns = [name for name in column_order if name not in description_map]
        used_names = set(stable_columns)
        final_map: Dict[str, str] = {}
        
        for code in rename_candidates:
            description = (description_map.get(code) or "").strip()
            if not description:
                continue
            
            final_name = self._dedupe_column_name(description, code, used_names)
            used_names.add(final_name)
            final_map[code] = final_name
        
        return final_map
    
    def _dedupe_column_name(self, candidate: str, code: str, used_names: set) -> str:
        if candidate not in used_names:
            return candidate
        
        preferred = f"{candidate} ({code})"
        if preferred not in used_names:
            return preferred
        
        suffix = 2
        while True:
            alt = f"{preferred} #{suffix}"
            if alt not in used_names:
                return alt
            suffix += 1
    
    def _rename_records(self, records: Optional[List[Dict[str, Any]]], rename_map: Dict[str, str]):
        if not records or not isinstance(records, list):
            return
        
        for record in records:
            if not isinstance(record, dict):
                continue
            updated = {}
            for key, value in record.items():
                new_key = rename_map.get(key, key)
                updated[new_key] = value
            record.clear()
            record.update(updated)
    
    def _rename_schema(self, schema: Optional[Dict[str, Any]], rename_map: Dict[str, str]):
        if not schema or not isinstance(schema, dict):
            return
        
        fields = schema.get("fields")
        if not fields or not isinstance(fields, list):
            return
        
        for field in fields:
            if not isinstance(field, dict):
                continue
            name = field.get("name")
            if name in rename_map:
                field["name"] = rename_map[name]
    
    def _record_metadata_overrides(
        self,
        result: Dict[str, Any],
        rename_map: Dict[str, str],
        description_map: Dict[str, str],
        dataset: str,
    ):
        metadata = result.setdefault("metadata", {})
        overrides = metadata.setdefault("column_name_overrides", {})
        overrides.update(rename_map)
        metadata.setdefault("column_description_source", "attr_name")
        metadata.setdefault("dataset", dataset)
        metadata.setdefault("attribute_descriptions", {})
        metadata["attribute_descriptions"].update({
            code: description_map.get(code)
            for code in rename_map.keys()
            if description_map.get(code)
        })
        metadata.setdefault("notes", [])
        note = f"Column names sourced from attr_name for dataset '{dataset}'"
        if note not in metadata["notes"]:
            metadata["notes"].append(note)


class CensusAttributeNameRepository:
    """
    Cached accessor for attr_name collection entries to map Census variables
    to their human-readable descriptions.
    """
    
    def __init__(self):
        self._client = MongoClient(Config.MONGO_URI)
        self._collection = self._client[Config.DATABASE_NAME]["attr_name"]
        self._cache: Dict[str, Dict[str, str]] = {}
    
    def get_descriptions(self, dataset: str, attribute_codes: List[str]) -> Dict[str, str]:
        if not dataset or not attribute_codes:
            return {}
        
        cached = self._cache.get(dataset)
        if cached is None:
            cached = self._load_dataset(dataset)
            self._cache[dataset] = cached
        
        descriptions = {}
        for code in attribute_codes:
            description = cached.get(code)
            if description:
                descriptions[code] = description
        return descriptions
    
    def _load_dataset(self, dataset: str) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        try:
            cursor = self._collection.find({"dataset": dataset})
            for doc in cursor:
                code = self._extract_code(doc)
                description = self._extract_description(doc)
                if code and description:
                    mapping[code] = description
        except Exception as exc:
            logger.warning("Failed loading attr_name dataset %s: %s", dataset, str(exc))
        return mapping
    
    @staticmethod
    def _extract_code(doc: Dict[str, Any]) -> Optional[str]:
        for key in ("attr_id", "attribute_id", "attribute_code", "code", "variable", "name"):
            value = doc.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None
    
    @staticmethod
    def _extract_description(doc: Dict[str, Any]) -> Optional[str]:
        for key in ("description", "attr_desc", "label", "long_name", "title", "attr_name"):
            value = doc.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None
