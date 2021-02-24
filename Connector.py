from error_handlers import *


class Connector:
    connected = False
    db = None
    user = None
    password = None
    host = None
    port = None

    def __init__(self, db, user, password, host, port):
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def __del__(self):
        pass

    # for 'all': [(1, 2, 3), ...] or []
    # for 'one': (1,2,3) or None
    def exec(self, request_str: str, identifiers: list = None, literals: list = None, result='all'):

        pass

    # {'books':[('id', 'integer'), ('date', 'date'), ...], ...}
    def table_info(self):
        pass

    def commit(self):
        pass
