# The Raw ElasticSearch functions, no frills, just wrappers around the HTTP calls

import requests, json
from models import Query

class Connection(object):
    def __init__(self, host, index, port=9200):
        self.host = host
        self.index = index
        self.port = port
        
        # make sure that host starts with "http://" or equivalent
        if not self.host.startswith("http"):
            self.host = "http://" + self.host
        
        # some people might tack the port onto the host
        if len(self.host.split(":")) > 2:
            self.port = self.host[self.host.rindex(":") + 1:]
            self.host = self.host[:self.host.rindex(":")]

def elasticsearch_url(connection, type=None, endpoint=None, params=None):
    index = connection.index
    host = connection.host
    port = connection.port

    # normalise the indexes input
    if index is None:
        index = "_all"
    if isinstance(index, list):
        index = ",".join(index)
    
    # normalise the types input
    if type is None:
        type = ""
    if isinstance(type, list):
        type = ",".join(type)
    
    # normalise the host
    if not host.startswith("http://"):
        host = "http://" + host
    if host.endswith("/"):
        host = host[:-1]
    
    if port is not None:
        host += ":" + str(port)
    host += "/"
    
    url = host + index
    if type is not None and type != "":
        url += "/" + type
    
    if endpoint is not None:
        if not url.endswith("/"):
            url += "/"
        url += endpoint
    
    # FIXME: NOT URL SAFE - do this properly
    if params is not None:
        args = []
        for k, v in params.iteritems():
            args.append(k + "=" + v)
        q = "&".join(args)
        url += "?" + q
    
    return url

def make_connection(connection, host, port, index):
    if connection is not None:
        return connection
    return Connection(host, index, port)

def search(connection, type=None, query=None):
    url = elasticsearch_url(connection, type, "_search")
    
    if query is None:
        query = Query.match_all()
    if not isinstance(query, dict):
        query = Query.query_string(query)
    
    resp = requests.post(url, data=json.dumps(query))
    return resp

def get(connection, type, id):
    url = elasticsearch_url(connection, type, endpoint=id)
    resp = requests.get(url)
    return resp

def mget(connection, type, ids, fields=None):
    if ids is None:
        raise ESWireException("mget requires one or more ids")
    docs = {"docs" : []}
    if fields is None:
        docs = {"ids" : ids}
    else:
        fields = [] if fields is None else fields if isinstance(fields, list) else [fields]
        for id in ids:
            docs["docs"].append({"_id" : id, "fields" : fields})
    url = elasticsearch_url(connection, type, endpoint="_mget")
    resp = requests.post(url, data=json.dumps(docs))
    return resp

def unpack_result(requests_response):
    j = requests_response.json()
    objects = [i.get("_source") if "_source" in i else i.get("fields") for i in j.get('hits', {}).get('hits', [])]
    return objects

def unpack_mget(requests_response):
    j = requests_response.json()
    objects = [i.get("_source") if "_source" in i else i.get("fields") for i in j.get("docs")]
    return objects

def unpack_get(requests_response):
    j = requests_response.json()
    return j.get("_source")

def put_mapping(connection, type=None, mapping=None, make_index=True):
    if mapping is None:
        raise ESWireException("cannot put empty mapping")
    
    if not index_exists(connection):
        if make_index:
            create_index(connection)
        else:
            raise ESWireException("index '" + str(connection.index) + "' does not exist")
    
    url = elasticsearch_url(connection, type, "_mapping")
    r = requests.put(url, json.dumps(mapping))
    return r

def has_mapping(connection, type):
    url = elasticsearch_url(connection, type, endpoint="_mapping")
    resp = requests.get(url)
    return resp.status_code == 200

def index_exists(connection):
    iurl = elasticsearch_url(connection, endpoint="_mapping")
    resp = requests.get(iurl)
    return resp.status_code == 200

def create_index(connection):
    iurl = elasticsearch_url(connection)
    resp = requests.post(iurl)
    return resp

def store(connection, type, record, id=None, params=None):
    url = elasticsearch_url(connection, type, endpoint=id, params=params)
    resp = None
    if id is not None:
        resp = requests.put(url, data=json.dumps(record))
    else:
        resp = requests.post(url, data=json.dumps(record))
    return resp

def delete(connection, type=None, id=None):
    url = elasticsearch_url(connection, type, endpoint=id)
    resp = requests.delete(url)
    return resp

def delete_by_query(connection, type, query):
    url = elasticsearch_url(connection, type, endpoint="_query")
    if "query" in query:
        # we have to unpack the query, as the endpoint covers that
        query = query["query"]
    resp = requests.delete(url, data=json.dumps(query))
    return resp

def refresh(connection):
    url = elasticsearch_url(connection, endpoint="_refresh")
    resp = requests.post(url)
    return resp

class ESWireException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
        
