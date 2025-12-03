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
    
    def find_one(self, query):
        for doc in self.find(query):
            return doc
        return None
    
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


def test_repository_returns_first_matching_variable_code():
    docs = [
        {"variable_code": "B22010_001E", "description": "Total households"},
        {"variable_code": "B22010_001E", "description": "Duplicate"},
    ]
    repo = _make_repo(docs)
    
    mapping = repo.get_descriptions(["B22010_001E"])
    
    assert mapping == {"B22010_001E": "Total households"}


def test_repository_handles_case_and_whitespace_matches():
    docs = [{
        "variable_code": "  b22010_002e ",
        "label": "SNAP households",
    }]
    repo = _make_repo(docs)
    
    mapping = repo.get_descriptions(["B22010_002E"])
    
    assert mapping == {"B22010_002E": "SNAP households"}


def test_repository_returns_empty_when_not_found():
    repo = _make_repo([])
    
    mapping = repo.get_descriptions(["B01001_999Z"])
    
    assert mapping == {}
