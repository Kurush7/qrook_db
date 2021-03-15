import psycopg2
import psycopg2.sql as sql

from IConnector import IConnector, DBResult
from error_handlers import log_error, retry_log_error
from threading import Lock
import symbols
import qrlogging

# IConnector realization for Postgres database
class PostgresConnector(IConnector):
    def __init__(self, db, user, password, host='localhost', port=5432):
        """
        configure connection
        :param db: db-name
        """
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connected = False

        self.conn = None
        self.lock = Lock()

        self.__connect()

    def __del__(self):
        if self.conn:
            self.conn.close()

    @retry_log_error()
    def __connect(self):
        self.conn = psycopg2.connect(dbname=self.db, user=self.user,
                                     password=self.password, host=self.host, port=self.port)
        self.cursor = self.conn.cursor()

    @log_error
    def exec(self, request: str, identifiers=None, literals=None, result='all'):
        request = request.replace(symbols.QRDB_IDENTIFIER, '{}')
        request = request.replace(symbols.QRDB_LITERAL, '%s')
        request = sql.SQL(request)
        if identifiers:
            identifiers = [sql.Identifier(x) for x in identifiers]
            request = request.format(*identifiers)

        qrlogging.info('POSTGRES EXECUTE: %s with literals %s', request.as_string(self.cursor), literals)
        with self.lock:
            self.cursor.execute(request, literals)
            data = self.extract_result(result)
        return DBResult(data, result)

    def extract_result(self, result):
        if result == 'all':
            return self.cursor.fetchall()
        elif result == 'one':
            return self.cursor.fetchone()
        elif result is not None:
            qrlogging.warning("unexpected 'result' value: %s" % result)
        return None

    @log_error
    def table_info(self):
        request = '''select table_name, column_name, data_type
                     from information_schema.columns
                     where table_schema = 'public';'''

        data = self.exec(request, result='all')
        info = {}
        for d in data.get_data():
            if not info.get(d[0]):
                info[d[0]] = []
            info[d[0]].append((d[1], d[2]))
        return info

    @log_error
    def commit(self):
        self.conn.commit()
