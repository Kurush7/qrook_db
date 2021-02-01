import psycopg2

from Connector import Connector
from error_handlers import log_error

class PostgresConnector(Connector):
    @log_error
    def __init__(self, db, user, password, host='localhost', port=5432):
        super().__init__(db, user, password, host, port)

        self.conn = psycopg2.connect(dbname=self.db, user=self.user,
                                     password=self.password, host=self.host, port=self.port)
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.__dict__.get('conn'):
            self.conn.close()

    def exec(self, sql: str):
        super().exec(sql)
        self.cursor.execute(sql)
        # todo check for fuckup

    @log_error
    def select(self, sql: str):
        super().select(sql)
        self.exec(sql)
        data = self.cursor.fetchall()   # todo security
        return data

    # {'books':[('id', 'integer'), ('date', 'date')]}
    @log_error
    def table_info(self):
        sql = "SELECT table_name, column_name, data_type  FROM information_schema.columns WHERE table_schema='public'"
        data = self.select(sql)
        info = {}
        for d in data:
            if not info.get(d[0]):
                info[d[0]] = []
            info[d[0]].append((d[1], d[2]))
        return info
