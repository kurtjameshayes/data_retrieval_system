import logging
import time
from typing import Dict, Any, List, Optional, Set

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
        
        # Capture the dataset for metadata bookkeeping and human-readable notes.
        dataset = self._extract_dataset_from_context(context)
        
        # Determine the columns we need to rename. Prefer the declared schema, fall back
        # to the first record when no schema is present.
        schema = result.get("schema")
        schema_fields = []
        if isinstance(schema, dict):
            fields = schema.get("fields")
            if isinstance(fields, list):
                schema_fields = [field for field in fields if isinstance(field, dict)]
        column_names = [
            field.get("name") for field in schema_fields if field.get("name")
        ]
        if not column_names:
            records = result.get("data")
            if isinstance(records, dict):
                records = records.get("data")
            if not isinstance(records, list):
                records = []
            first_record = next(
                (record for record in records if isinstance(record, dict) and record),
                None,
            )
            if not first_record:
                return result
            column_names = list(first_record.keys())
        
        # Fetch friendly descriptions. If nothing is returned we can stop early.
        description_map = self._attribute_repository.get_descriptions(column_names)
        if not description_map:
            return result
        
        # Build the rename map while keeping column order intact and avoiding collisions.
        used_names: Set[str] = set(column_names)
        rename_map: Dict[str, str] = {}
        for code in column_names:
            description = (description_map.get(code) or "").strip()
            if not description:
                continue
            final_name = self._dedupe_column_name(description, code, used_names)
            if final_name == code:
                continue
            rename_map[code] = final_name
            used_names.add(final_name)
        
        if not rename_map:
            return result
        
        # Rename every record in-place so downstream DataFrame builders see the
        # human-friendly columns.
        records = result.get("data")
        if isinstance(records, dict):
            records = records.get("data")
        if isinstance(records, list):
            for record in records:
                if not isinstance(record, dict):
                    continue
                for original, friendly in rename_map.items():
                    if original in record:
                        record[friendly] = record.pop(original)
        
        # Keep schema definitions aligned with the renamed records when possible.
        for field in schema_fields:
            original_name = field.get("name")
            if original_name in rename_map:
                field["name"] = rename_map[original_name]
        
        # Record metadata so UI layers can explain where the names came from.
        metadata = result.setdefault("metadata", {})
        overrides = metadata.setdefault("column_name_overrides", {})
        overrides.update(rename_map)
        metadata.setdefault("column_description_source", "attr_name")
        metadata.setdefault("attribute_descriptions", {})
        metadata["attribute_descriptions"].update({
            code: description_map.get(code)
            for code in rename_map.keys()
            if description_map.get(code)
        })
        if dataset:
            metadata.setdefault("dataset", dataset)
        metadata.setdefault("notes", [])
        note = (
            f"Column names sourced from attr_name for dataset '{dataset}'"
            if dataset
            else "Column names sourced from attr_name"
        )
        if note not in metadata["notes"]:
            metadata["notes"].append(note)
        
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
    


class CensusAttributeNameRepository:
    """
    Cached accessor for attr_name collection entries to map Census variables
    to their human-readable descriptions.
    """
    
    _CODE_FIELDS = (
        "attr_id",
        "attribute_id",
        "attribute_code",
        "code",
        "variable",
        "name",
    )
    
    def __init__(self):
        self._client = MongoClient(Config.MONGO_URI)
        self._collection = self._client[Config.DATABASE_NAME]["attr_name"]
        self._cache: Dict[str, Optional[str]] = {}
    
    def get_descriptions(self, attribute_codes: List[str]) -> Dict[str, str]:
        """
        Retrieve descriptions for the provided Census variable codes.
        
        The attr_name collection may contain multiple datasets, but lookups
        only consider the variable code to ensure the widest possible match.
        """
        normalized = [
            code.strip()
            for code in attribute_codes
            if isinstance(code, str) and code.strip()
        ]
        if not normalized:
            return {}
        
        descriptions: Dict[str, str] = {}
        missing: List[str] = []
        
        for code in normalized:
            if code in descriptions:
                continue
            
            if code in self._cache:
                cached = self._cache[code]
                if cached:
                    descriptions[code] = cached
            else:
                missing.append(code)
        
        if missing:
            pending_codes = list(dict.fromkeys(missing))
            fetched = self._load_by_codes(pending_codes)
            for code, description in fetched.items():
                self._cache[code] = description
                if description:
                    descriptions[code] = description
            
            for code in pending_codes:
                self._cache.setdefault(code, fetched.get(code))
        
        return descriptions
    
    def _load_by_codes(self, codes: List[str]) -> Dict[str, Optional[str]]:
        mapping: Dict[str, Optional[str]] = {}
        if not codes:
            return mapping
        
        normalized_codes = [code.strip() for code in codes if isinstance(code, str) and code.strip()]
        if not normalized_codes:
            return mapping
        
        code_set: Set[str] = set(normalized_codes)
        query = self._build_code_query(list(code_set))
        if not query:
            return mapping
        
        try:
            cursor = self._collection.find(query)
            for doc in cursor:
                matches = self._extract_matching_codes(doc, code_set)
                if not matches:
                    continue
                
                description = self._extract_description(doc)
                for match in matches:
                    mapping.setdefault(match, description)
        except Exception as exc:
            logger.warning("Failed loading attr_name codes %s: %s", codes, str(exc))
        return mapping
    
    def _build_code_query(self, codes: List[str]) -> Optional[Dict[str, Any]]:
        clauses = [{field: {"$in": codes}} for field in self._CODE_FIELDS]
        if not clauses:
            return None
        return {"$or": clauses}
    
    @staticmethod
    def _extract_matching_codes(doc: Dict[str, Any], code_set: Set[str]) -> List[str]:
        matches: List[str] = []
        for key in ("attr_id", "attribute_id", "attribute_code", "code", "variable", "name"):
            value = doc.get(key)
            if not isinstance(value, str):
                continue
            candidate = value.strip()
            if candidate and candidate in code_set:
                matches.append(candidate)
        return matches
    
    @staticmethod
    def _extract_description(doc: Dict[str, Any]) -> Optional[str]:
        for key in ("description", "attr_desc", "label", "long_name", "title", "attr_name"):
            value = doc.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None
