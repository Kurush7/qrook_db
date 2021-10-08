import sqlite3
import psycopg2.sql as sql
from threading import Lock

from IConnector import IConnector, DBResult
from error_handlers import log_error, retry_log_error
from threading import Lock
import symbols
import qrlogging
import threading


class SQLiteConnector(IConnector):
    """
    IConnector realization for Postgres database. Uses sqlite; multi-threading is not supported, mutexes are used
    """
    mutex = Lock()

    def __init__(self, db):
        """
        configure connection
        :param db: db-name
        """
        self.db = db
        self.connected = False

        self.pool = None
        self.__connect()

    @retry_log_error()
    def __connect(self):
        self.conn = sqlite3.connect(self.db)
        self.cursor = self.conn.cursor()

    def exec(self, request: str, identifiers=None, literals=None, result='all'):

        request = request.replace(symbols.QRDB_LITERAL, '?')
        if identifiers:
            identifiers = ['"' + x + '"' for x in identifiers]
            i = 0
            while request.find(symbols.QRDB_IDENTIFIER) != -1:
                request = request.replace(symbols.QRDB_IDENTIFIER, identifiers[i], 1)
                i += 1
            if len(identifiers) != i:
                raise Exception('unexpected identifiers count')

        qrlogging.info('SQLite EXECUTE: %s with literals %s', request, literals)
        if literals is None:
            literals = []
        try:
            self.mutex.acquire()
            self.cursor.execute(request, literals)
        except Exception as e:
            self.mutex.release()
            raise e
        data = self.extract_result(result)
        self.mutex.release()
        return DBResult(data, result)

    def extract_result(self, result):
        if result == 'all':
            return self.cursor.fetchall()
        elif result == 'one':
            return self.cursor.fetchone()
        elif result is not None:
            qrlogging.warning("unexpected 'result' value: %s" % result)
        return None

    def table_info(self):
        sql = '''SELECT name FROM sqlite_master where type = 'table';'''

        tables = self.exec(sql, result='all')
        info = {}
        for table in tables.get_data():
            name = table[0]
            sql = 'PRAGMA table_info(%s);' % name
            data = self.exec(sql, result='all')
            info[name] = []
            for d in data.get_data():
                info[name].append([d[1], d[2]])
        return info

    def commit(self):
        self.conn.commit()
