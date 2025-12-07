import logging
import re
import time
from typing import Dict, Any, List, Optional, Set

import requests
from pymongo import MongoClient

from config import Config
from core.base_connector import BaseConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InvalidCensusApiKeyError(Exception):
    """Raised when the Census API reports that an API key is invalid."""
    pass

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
        self._api_key_valid = bool(self.api_key)
        self._invalid_key_warned = False
    
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
        test_url = f"{self.base_url}/2020/acs/acs5"
        params = {
            "get": "NAME",
            "for": "state:01",
        }
        use_key = bool(self.api_key) and self._api_key_valid
        if use_key:
            params["key"] = self.api_key

        try:
            response = requests.get(test_url, params=params, timeout=10)
            if response.status_code != 200:
                logger.error(
                    "Validation failed with status %s: %s",
                    response.status_code,
                    response.text,
                )
                return False
            self._decode_response_payload(
                response,
                attempted_with_key=use_key,
            )
            return True
        except InvalidCensusApiKeyError:
            self._handle_invalid_api_key()
            params.pop("key", None)
            try:
                response = requests.get(test_url, params=params, timeout=10)
                if response.status_code != 200:
                    logger.error(
                        "Validation without API key failed with status %s: %s",
                        response.status_code,
                        response.text,
                    )
                    return False
                self._decode_response_payload(
                    response,
                    attempted_with_key=False,
                )
                return True
            except Exception as exc:
                logger.error(f"Validation failed after removing API key: {exc}")
                return False
        except Exception as exc:
            logger.error(f"Validation failed: {exc}")
            return False
    
    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute query against Census.gov API.
        """
        if not self.connected:
            self.connect()

        dataset = parameters.get("dataset")
        if not dataset:
            raise ValueError("Dataset parameter is required")

        query_url = f"{self.base_url}/{dataset}"
        query_params = {k: v for k, v in parameters.items() if k != "dataset"}

        use_api_key = bool(self.api_key) and self._api_key_valid
        if use_api_key:
            query_params["key"] = self.api_key

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    query_url,
                    params=query_params,
                    timeout=30,
                )

                if response.status_code == 200:
                    payload = self._decode_response_payload(
                        response,
                        attempted_with_key=use_api_key,
                    )
                    return self.transform(payload)

                if response.status_code == 429:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        "Rate limited. Waiting %ss before retry...",
                        wait_time,
                    )
                    time.sleep(wait_time)
                    continue

                raise Exception(
                    f"API error: {response.status_code} - {response.text}"
                )

            except InvalidCensusApiKeyError:
                if use_api_key:
                    self._handle_invalid_api_key()
                    query_params.pop("key", None)
                    use_api_key = False
                    continue
                raise
            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt < self.max_retries - 1:
                    logger.warning(
                        "Timeout on attempt %s. Retrying...",
                        attempt + 1,
                    )
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
            except Exception as exc:
                last_error = exc
                if attempt < self.max_retries - 1:
                    logger.warning(
                        "Error on attempt %s: %s. Retrying...",
                        attempt + 1,
                        str(exc),
                    )
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise

        raise last_error or Exception("Max retries exceeded")
    
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

    def _decode_response_payload(
        self,
        response: requests.Response,
        *,
        attempted_with_key: bool,
    ) -> Any:
        body = response.text or ""
        stripped = body.lstrip()
        if attempted_with_key and self._looks_like_invalid_key(body):
            raise InvalidCensusApiKeyError(
                "Census API rejected the configured API key."
            )
        if not stripped:
            raise ValueError("Census API returned an empty response body.")

        content_type = (response.headers.get("content-type") or "").lower()
        looks_json = "json" in content_type or stripped.startswith(("[", "{"))

        if looks_json:
            try:
                return response.json()
            except ValueError as exc:
                raise ValueError(
                    f"Unable to parse Census API response as JSON: {exc}"
                ) from exc

        raise ValueError(
            "Census API returned unexpected content "
            f"type '{content_type or 'unknown'}': {body[:200]!r}"
        )

    @staticmethod
    def _looks_like_invalid_key(payload: str) -> bool:
        snippet = payload[:512].lower()
        return "invalid key" in snippet or "key is not valid" in snippet

    def _handle_invalid_api_key(self) -> None:
        if not self._api_key_valid:
            return
        self._api_key_valid = False
        if not self._invalid_key_warned:
            logger.warning(
                "Census API rejected the configured API key; continuing without it."
            )
            self._invalid_key_warned = True
    
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
        
        attribute_descriptions = {
            code: desc
            for code, desc in description_map.items()
            if self._is_valid_description(desc)
        }
        
        # Build the rename map while keeping column order intact and avoiding collisions.
        used_names: Set[str] = set(column_names)
        rename_map: Dict[str, str] = {}
        conflicts: List[str] = []
        for code in column_names:
            description = description_map.get(code)
            if not self._is_valid_description(description):
                continue
            candidate_name = description  # Preserve attr_name wording exactly.
            if candidate_name not in used_names:
                rename_map[code] = candidate_name
                used_names.add(candidate_name)
            else:
                conflicts.append(code)
        
        metadata = result.setdefault("metadata", {})
        metadata.setdefault("column_description_source", "attr_name")
        if attribute_descriptions:
            metadata.setdefault("attribute_descriptions", {}).update(attribute_descriptions)
        if conflicts:
            conflict_store = metadata.setdefault("column_name_conflicts", [])
            for code in conflicts:
                if code not in conflict_store:
                    conflict_store.append(code)
        
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
    
    @staticmethod
    def _is_valid_description(value: Optional[Any]) -> bool:
        return isinstance(value, str) and bool(value.strip())
    


class CensusAttributeNameRepository:
    """
    Cached accessor for attr_name collection entries to map Census variables
    to their human-readable descriptions.
    """
    
    def __init__(self):
        self._client = MongoClient(Config.MONGO_URI)
        self._collection = self._client[Config.DATABASE_NAME]["attr_name"]
        self._cache: Dict[str, Optional[str]] = {}
    
    def get_descriptions(self, attribute_codes: List[str]) -> Dict[str, str]:
        """
        Retrieve descriptions for the provided Census variable codes by looking
        up the first attr_name record whose variable_code matches the
        attribute_code we are trying to label.
        """
        normalized = [
            code.strip()
            for code in attribute_codes
            if isinstance(code, str) and code.strip()
        ]
        if not normalized:
            return {}
        
        descriptions: Dict[str, str] = {}
        for code in normalized:
            if code in descriptions:
                continue
            
            if code in self._cache:
                description = self._cache[code]
            else:
                description = self._lookup_description_by_variable_code(code)
                self._cache[code] = description
            
            if description:
                descriptions[code] = description
        
        return descriptions
    
    def _lookup_description_by_variable_code(self, code: str) -> Optional[str]:
        if not code:
            return None
        
        pattern = rf"^\s*{re.escape(code)}\s*$"
        queries = [
            {"variable_code": code},
            {"variable_code": {"$regex": pattern, "$options": "i"}},
        ]
        
        for query in queries:
            try:
                doc = self._collection.find_one(query)
            except Exception as exc:
                logger.warning(
                    "Failed attr_name lookup for %s using %s: %s",
                    code,
                    query,
                    str(exc),
                )
                return None
            
            if doc:
                description = self._extract_description(doc)
                if description:
                    return description
        return None
    
    @staticmethod
    def _extract_description(doc: Dict[str, Any]) -> Optional[str]:
        for key in ("description", "attr_desc", "label", "long_name", "title", "attr_name"):
            value = doc.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None
