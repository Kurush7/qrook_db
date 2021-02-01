# todo add load from config
# todo commit method
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

    def exec(self, sql: str):
        pass

    # [(1, 2, 3), ...]
    def select(self, sql: str):
        pass

    # {'books':[('id', 'integer'), ('date', 'date')]}
    def table_info(self):
        pass
