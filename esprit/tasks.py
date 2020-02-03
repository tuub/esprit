from esprit import raw, models
import json, sys

class ScrollException(Exception):
    pass

def copy(source_conn, source_type, target_conn, target_type, limit=None, batch_size=1000, method="POST", q=None):
    if q is None:
        q = models.QueryBuilder.match_all()
    batch = []
    for r in iterate(source_conn, source_type, q, page_size=batch_size, limit=limit, method=method):
        batch.append(r)
        if len(batch) >= batch_size:
            print(("writing batch of", len(batch)))
            raw.bulk(target_conn, target_type, batch)
            batch = []
    if len(batch) > 0:
        print(("writing batch of", len(batch)))
        raw.bulk(target_conn, target_type, batch)

# 2018-12-19 TD : raise keepalive value to '10m'
# def scroll(conn, type, q=None, page_size=1000, limit=None, keepalive="1m"):
def scroll(conn, type, q=None, page_size=1000, limit=None, keepalive="10m"):
    if q is not None:
        q = q.copy()
    if q is None:
        q = {"query" : {"match_all" : {}}}
    if "size" not in q:
        q["size"] = page_size
    if "sort" not in q: # to ensure complete coverage on a changing index, sort by id is our best bet
        q["sort"] = [{"id" : {"order" : "asc"}}]

    resp = raw.initialise_scroll(conn, type, q, keepalive)
    if resp.status_code != 200:
        # something went wrong initialising the scroll
        raise ScrollException("Unable to initialise scroll - could be your mappings are broken")

    # otherwise, carry on
    results, scroll_id = raw.unpack_scroll(resp)

    counter = 0
    for r in results:
        # apply the limit
        if limit is not None and counter >= int(limit):
            break
        counter += 1
        yield r

    while True:
        # apply the limit
        if limit is not None and counter >= int(limit):
            break

        sresp = raw.scroll_next(conn, scroll_id, keepalive=keepalive)
        if raw.scroll_timedout(sresp):
            raise ScrollException("scroll timed out - you probably need to raise the keepalive value")
        results = raw.unpack_result(sresp)

        if len(results) == 0:
            break
        for r in results:
            # apply the limit (again)
            if limit is not None and counter >= int(limit):
                break
            counter += 1
            yield r

def iterate(conn, type, q, page_size=1000, limit=None, method="POST"):
    q = q.copy()
    q["size"] = page_size
    q["from"] = 0
    if "sort" not in q: # to ensure complete coverage on a changing index, sort by id is our best bet
        q["sort"] = [{"id" : {"order" : "asc"}}]
    counter = 0
    while True:
        # apply the limit
        if limit is not None and counter >= int(limit):
            break
        
        res = raw.search(conn, type=type, query=q, method=method)
        rs = raw.unpack_result(res)
        
        if len(rs) == 0:
            break
        for r in rs:
            # apply the limit (again)
            if limit is not None and counter >= int(limit):
                break
            counter += 1
            yield r
        q["from"] += page_size

def dump(conn, type, q=None, page_size=1000, limit=None, method="POST", out=None, transform=None):
    q = q if q is not None else {"query" : {"match_all" : {}}}
    out = out if out is not None else sys.stdout
    for record in iterate(conn, type, q, page_size=page_size, limit=limit, method=method):
        if transform is not None:
            record = transform(record)
        out.write(json.dumps(record))

class JSONListWriter(object):
    def __init__(self, path):
        self.f = open(path, "wb")
        self.f.write("[")
        self.first = True

    def write(self, serialised_json_object):
        if self.first:
            self.first = False
        else:
            self.f.write(",")
        self.f.write(serialised_json_object)

    def close(self):
        self.f.write("]")
        self.f.close()
