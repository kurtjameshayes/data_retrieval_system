import re

from connectors.census.connector import CensusAttributeNameRepository


class FakeCollection:
    def __init__(self, docs):
        self._docs = docs
        self.last_query = None
    
    def find(self, query):
        self.last_query = query
        for doc in self._docs:
            if not query or self._matches(doc, query):
                yield doc
    
    def _matches(self, doc, query):
        if not query:
            return True
        
        if "$or" in query:
            return any(self._matches(doc, clause) for clause in query["$or"])
        
        for field, condition in query.items():
            value = doc.get(field)
            if isinstance(condition, dict):
                if "$in" in condition:
                    if not isinstance(value, str):
                        return False
                    if value not in condition["$in"]:
                        return False
                elif "$regex" in condition:
                    if not isinstance(value, str):
                        return False
                    flags = re.IGNORECASE if "i" in condition.get("$options", "") else 0
                    if not re.match(condition["$regex"], value, flags):
                        return False
                else:
                    raise ValueError(f"Unsupported query condition {condition}")
            else:
                if value != condition:
                    return False
        
        return True


def _make_repo(docs):
    repo = object.__new__(CensusAttributeNameRepository)
    repo._client = None
    repo._collection = FakeCollection(docs)
    repo._cache = {}
    return repo


def test_repository_matches_attribute_code_when_attr_id_differs():
    docs = [{
        "attr_id": "random_id",
        "attribute_code": "B22010_001E",
        "description": "Total households",
    }]
    repo = _make_repo(docs)
    
    mapping = repo.get_descriptions(["B22010_001E"])
    
    assert mapping == {"B22010_001E": "Total households"}


def test_repository_collects_multiple_matching_fields_once():
    docs = [{
        "attr_id": "something_else",
        "attribute_code": "B22010_002E",
        "code": "B22010_002E",
        "label": "SNAP households",
    }]
    repo = _make_repo(docs)
    
    mapping = repo.get_descriptions(["B22010_002E", "NAME"])
    
    assert mapping == {"B22010_002E": "SNAP households"}


def test_repository_handles_codes_with_trailing_whitespace():
    docs = [{
        "attribute_code": "B22010_003E   ",
        "description": "Households with whitespace",
    }]
    repo = _make_repo(docs)
    
    mapping = repo.get_descriptions(["B22010_003E"])
    
    assert mapping == {"B22010_003E": "Households with whitespace"}


def test_repository_handles_case_insensitive_codes():
    docs = [{
        "attribute_code": "b22010_004e",
        "long_name": "Case insensitive households",
    }]
    repo = _make_repo(docs)
    
    mapping = repo.get_descriptions(["B22010_004E"])
    
    assert mapping == {"B22010_004E": "Case insensitive households"}
