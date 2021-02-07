import psycopg2
import psycopg2.sql as sql

from Connector import Connector
from error_handlers import log_error, retry_log_error


class PostgresConnector(Connector):
    def __init__(self, db, user, password, host='localhost', port=5432):
        super().__init__(db, user, password, host, port)
        self.__connect()

    def __del__(self):
        if self.__dict__.get('conn'):
            self.conn.close()

    @retry_log_error()
    def __connect(self):
        self.conn = psycopg2.connect(dbname=self.db, user=self.user,
                                     password=self.password, host=self.host, port=self.port)
        self.cursor = self.conn.cursor()

    @log_error
    def exec(self, request: str, identifiers: list = None, literals: list = None, result='all'):
        super().exec(request, identifiers, literals, result)
        if identifiers:
            identifiers = [sql.Identifier(x) for x in identifiers]
            request = sql.SQL(request).format(*identifiers)

        self.cursor.execute(request, literals)
        return self.extract_result(result)

    def extract_result(self, result):
        if result == 'all':
            return self.cursor.fetchall()
        elif result == 'one':
            return self.cursor.fetchone()
        return None

    @log_error
    def table_info(self):
        request = "select table_name, column_name, data_type  " \
                  "from information_schema.columns " \
                  "where table_schema = 'public'"
        data = self.exec(request, result='all')
        info = {}
        for d in data:
            if not info.get(d[0]):
                info[d[0]] = []
            info[d[0]].append((d[1], d[2]))
        return info

    @log_error
    def commit(self):
        self.conn.commit()
