from copy import deepcopy

class Query(object):
    _match_all = { "query" : { "match_all" : {} }}
    _query_string = {"query" : {"query_string" : {"query" : "<query string>"}}}
    _term = {"query": {"term": { } } }
    
    @classmethod
    def match_all(cls):
        return deepcopy(cls._match_all)
    
    @classmethod
    def query_string(cls, query):
        q = deepcopy(cls._query_string)
        q["query"]["query_string"]["query"] = query
        return q
        
    @classmethod
    def term(cls, key, value):
        q = deepcopy(cls._term) 
        q["query"]["term"][key] = value
        return q

