import uuid
from esprit import raw, util

class StoreException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class DAO(object):
    
    @classmethod
    def makeid(cls):
        return uuid.uuid4().hex
    
    def actions(self, conn, action_queue):
        for action in action_queue:
            if action.keys()[0] == "remove":
                self._action_remove(conn, action)
            elif action.keys()[0] == "store":
                self._action_store(conn, action)
    
    def _action_remove(self, conn, remove_action):
        obj = remove_action.get("remove")
        if "index" not in obj:
            raise StoreException("no index provided for remove action")
        if "id" not in obj and "query" not in obj:
            raise StoreException("no id or query provided for remove action")
        if "id" in obj:
            raw.delete(conn, obj.get("index"), obj.get("id"))
        elif "query" in obj:
            raw.delete_by_query(conn, obj.get("index"), obj.get("query"))

    def _action_store(self, conn, store_action):
        obj = store_action.get("store")
        if "index" not in obj:
            raise StoreException("no index provided for store action")
        if "record" not in obj:
            raise StoreException("no record provided for store action")
        raw.store(conn, obj.get("index"), obj.get("record"), obj.get("id"))
    
class DomainObject(DAO):
    __type__ = None
    __conn__ = None
    
    def __init__(self, raw):
        self.data = raw
    
    @property
    def id(self):
        return self.data.get('id', None)
    
    @property
    def json(self):
        return json.dumps(self.data)
    
    def save(self, conn=None):
        if 'created_date' not in self.data:
            self.data['created_date'] = util.now()
        self.data['last_updated'] = util.now()
        
        if conn is None:
            conn = self.__conn__
        raw.store(conn, self.__type__, self.data, self.id)
        
    
    
