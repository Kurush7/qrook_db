import psycopg2
import psycopg2.sql as sql
from psycopg2 import pool

from IConnector import IConnector, DBResult
from error_handlers import log_error, retry_log_error
from threading import Lock
import symbols
import qrlogging
import threading


class PostgresConnector(IConnector):
    """
    IConnector realization for Postgres database. Uses psycorg2 and connection pools for multi-threaded usage
    """

    def __init__(self, db, user, password, host='localhost', port=5432, schema='public', min_conn=1, max_conn=10):
        """
        configure connection
        :param db: db-name
        """
        self.db = db
        self.schema = schema
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connected = False
        self.min_conn = min_conn
        self.max_conn = max_conn

        self.enable_drop = False
        self.pool = None
        self.__connect()

    def __del__(self):
        if self.pool:
            if not self.pool.closed:
                self.pool.closeall()

    def enable_database_drop(self) -> bool:
        self.enable_drop = True
        return True

    @retry_log_error()
    def __connect(self):
        self.pool = psycopg2.pool.ThreadedConnectionPool(self.min_conn, self.max_conn, dbname=self.db, user=self.user,
                                     password=self.password, host=self.host, port=self.port)

    def exec(self, request: str, identifiers=None, literals=None, result='all'):
        conn = self.pool.getconn(key=threading.get_ident())
        cursor = conn.cursor()
        if self.enable_drop:
            conn.autocommit = True
            conn.set_isolation_level(0)
        #request = request.replace(symbols.QRDB_IDENTIFIER, '{}')
        request = request.replace(symbols.QRDB_LITERAL, '%s')
        if identifiers:
            #identifiers = [sql.Identifier(x) for x in identifiers]
            identifiers = ['"' + x + '"' for x in identifiers]
            i = 0
            while request.find(symbols.QRDB_IDENTIFIER) != -1:
                request = request.replace(symbols.QRDB_IDENTIFIER, identifiers[i], 1)
                i += 1
            if len(identifiers) != i:
                raise Exception('unexpected identifiers count')
            #request = request.format(*identifiers)

        request = sql.SQL(request)
        qrlogging.info('POSTGRES EXECUTE: %s with literals %s', request.as_string(cursor), literals)
        try:
            #cursor.execute('''SET search_path TO %s, public;''' % self.schema)
            cursor.execute(request, literals)
        except Exception as e:
            conn.rollback()
            raise e
        data = self.extract_result(cursor, result)
        cursor.close()
        #if request.startswith('select'):
        if conn.autocommit:
            self.pool.putconn(conn, key=threading.get_ident())
        return DBResult(data, result)

    def extract_result(self, cursor, result):
        if result == 'all':
            return cursor.fetchall()
        elif result == 'one':
            return cursor.fetchone()
        elif result is not None:
            qrlogging.warning("unexpected 'result' value: %s" % result)
        return None

    def table_info(self):
        request = '''
        select distinct table_name, column_name, data_type
                     from information_schema.columns
                     where table_schema = '%s';
         ''' % self.schema

        data = self.exec(request, result='all')
        info = {}
        for d in data.get_data():
            if not info.get(d[0]):
                info[d[0]] = {'columns': []}
            info[d[0]]['columns'].append((d[1], d[2]))

        self._add_table_constraints(info)
        return info

    def _add_table_constraints(self, tables: dict):
        request = '''
        SELECT
            tc.constraint_type,
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type IN ('FOREIGN KEY', 'PRIMARY KEY') and tc.table_schema='%s';
         ''' % self.schema
        data = self.exec(request, result='all')

        for constraint, table, column, foreign_table, foreign_column in data.get_data():
            if constraint == 'PRIMARY KEY':
                tables[table]['primary_key'] = column
            if constraint == 'FOREIGN KEY':
                if not tables[table].get('foreign_keys'):
                    tables[table]['foreign_keys'] = []
                tables[table]['foreign_keys'].append({'column': column,
                                                      'foreign_table': foreign_table,
                                                      'foreign_column': foreign_column
                                                      })

    def commit(self):
        conn = self.pool.getconn(key=threading.get_ident())
        conn.commit()
        self.pool.putconn(conn, key=threading.get_ident())
