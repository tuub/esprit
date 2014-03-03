from esprit import raw, models

def copy(source_conn, source_type, target_conn, target_type, limit=None, batch_size=1000):
    q = models.Query.match_all()
    batch = []
    for r in iterate(source_conn, source_type, q, page_size=batch_size, limit=limit):
        batch.append(r)
        if len(batch) >= batch_size:
            resp = raw.bulk(target_conn, target_type, batch)
            batch = []
    if len(batch) > 0:
        resp = raw.bulk(target_conn, target_type, batch)

def iterate(conn, type, q, page_size=1000, limit=None):
    q["size"] = page_size
    q["from"] = 0
    counter = 0
    while True:
        # apply the limit
        if limit is not None and counter >= limit:
            break
        
        res = raw.search(conn, type=type, query=q)
        rs = raw.unpack_result(res)
        
        if len(rs) == 0:
            break
        for r in rs:
            # apply the limit (again)
            if limit is not None and counter >= limit:
                break
            counter += 1
            yield r
        q["from"] += page_size
