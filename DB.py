import DI
import sys
from request import *
from request_new import *
# todo add exec raw query
# todo add support for nested queries
# todo add select(books.id) parsing -> extract table from ONLY datafield


class DBCreator:
    """
    Class responsible for objects creation
    """

    # todo db is facade... use some query_aggregator instead
    def __init__(self, conn: IConnector, db):
        """
        :param conn: active connection for tables to use
        :param db: todo
        """
        self.conn = conn
        self.db = db

    def create_data(self, source=None, in_module=False):
        tables = self.conn.table_info()
        t = dict()

        for name, field in tables.items():
            t[name] = QRTable(name, field, self.db)

        self.__dict__.update(t)
        if source:
            if in_module:
                source = sys.modules[source]
            source.__dict__.update(t)


@log_class(log_error)
class DB:
    @log_error
    # format: one of 'list', 'dict'
    # todo rename format param
    def __init__(self, connector_type, *conn_args, format=None, **conn_kwargs):
        DI.register(format, connector_type, *conn_args, **conn_kwargs)

        self.meta = dict()
        self.meta['connector'] = inject.instance(IConnector)

    # source is object with __dict__ field or a module name (with 'in_module' flag up)
    def create_data(self, source=None, in_module=False):
        DBCreator(self.meta['connector'], self).create_data(source, in_module)

    def commit(self):
        self.meta['connector'].commit()

    def exec(self, raw_query):
        #logger.warning('UNSAFE: executing raw query: %s', raw_query)
        return QRequest(self.meta['connector'], request=raw_query)

    def select(self, table: QRTable, *args):
        return QRSelect(self.meta['connector'], table, *args)

    def delete(self, table: QRTable, auto_commit=False):
        return QRDelete(self.meta['connector'], table, auto_commit)

    def update(self, table: QRTable, auto_commit=False):
        return QRUpdate(self.meta['connector'], table, auto_commit)

    def insert(self, table: QRTable, *args, auto_commit=False):
        return QRInsert(self.meta['connector'], table, *args, auto_commit=auto_commit)

