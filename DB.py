import DI
import sys
from query import *
import operators

# todo-list throughout the orm-project
# important
# todo tests

# middle
# todo insert values -> add array of dicts support

# future
# todo add support for nested queries
# todo add select(books.id) parsing -> extract table from ONLY datafield
# todo add returning part for delete query (and update?)
# todo add StructDataFormatter
# todo add 'as' syntax: 'select count(*) as cnt


import qrlogging

class DBQueryAggregator:
    def __init__(self, conn: IConnector):
        self.connector = conn

    def exec(self, raw_query):
        return QRExec(self.connector, raw_query)

    def select(self, table: QRTable, *args, **kwargs):
        return QRSelect(self.connector, table, *args)

    def delete(self, table: QRTable, auto_commit=False, *args, **kwargs):
        return QRDelete(self.connector, table, auto_commit)

    def update(self, table: QRTable, auto_commit=False, *args, **kwargs):
        return QRUpdate(self.connector, table, auto_commit)

    def insert(self, table: QRTable, *args, auto_commit=False, **kwargs):
        return QRInsert(self.connector, table, *args, auto_commit=auto_commit)


class DBCreator:
    """
    Class responsible for objects creation
    """

    def __init__(self, conn: IConnector, db: DBQueryAggregator):
        """
        :param conn: active connection for tables to use
        :param db: query aggregator
        """
        self.conn = conn
        self.db = db

    def create_data(self, source=None, in_module=False):
        """creates table data (described in DB.create_data) and returns it"""
        tables = self.conn.table_info()
        t = dict()

        for name, field in tables.items():
            t[name] = QRTable(name, field, self.db)

        self.__dict__.update(t)
        if source:
            if in_module:
                source = sys.modules[source]
            source.__dict__.update(t)
        return t


class DB:
    operators = operators

    def __init__(self, connector_type, *conn_args, format_type=None, **conn_kwargs):
        """
        :param connector_type: now only 'postgres' is supported
        :param conn_args: arg-params like host and port needed to connect to database
        :param format_type: the format data will be returned in; now only 'list' and 'dict' are supported
        :param conn_kwargs: kwarg-params like host and port needed to connect to database
        """
        DI.register(format_type, connector_type, *conn_args, **conn_kwargs)

        self.meta = dict()
        self.meta['connector'] = inject.instance(IConnector)
        self.meta['aggregator'] = DBQueryAggregator(self.meta['connector'])

    def create_logger(self, logger_name='default', app_name='app',
                  level="INFO", file: str = None, file_level="INFO"):
        qrlogging.logger = qrlogging.create_logger(logger_name, app_name, level, file, file_level)

    def create_data(self, source=None, in_module=False):
        """ Create QRTable objects for all tables in given database (use system tables data)
            and stores these objects in this class.
            if 'source' present, attributes with names of found tables will be added to it
            'source' is object with __dict__ field or a module name (with 'in_module' flag up)
        """
        data = DBCreator(self.meta['connector'], self.meta['aggregator']).create_data(source, in_module)
        self.__dict__.update(data)

    def commit(self):
        self.meta['connector'].commit()

    def exec(self, raw_query):
        return self.meta['aggregator'].exec(raw_query)

    def select(self, table: QRTable, *args):
        return self.meta['aggregator'].select(table, *args)

    def delete(self, table: QRTable, auto_commit=False):
        return self.meta['aggregator'].delete(table, auto_commit)

    def update(self, table: QRTable, auto_commit=False):
        return self.meta['aggregator'].update(table, auto_commit)

    def insert(self, table: QRTable, *args, auto_commit=False):
        return self.meta['aggregator'].insert(table, *args, auto_commit=auto_commit)

