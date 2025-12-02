from connectors.census.connector import CensusAttributeNameRepository


class FakeCollection:
    def __init__(self, docs):
        self._docs = docs
        self.last_query = None
    
    def find(self, query):
        self.last_query = query
        for doc in self._docs:
            yield doc


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
