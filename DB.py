import PostgresConnector
from error_handlers import *
from data import *
from request import *
import sys

# todo add exec raw query

class DB:
    @log_error
    def __init__(self, connector_type, *conn_args, **conn_kwargs):
        self.meta = dict()
        if connector_type == 'postgres':
            self.meta['connector'] = PostgresConnector.PostgresConnector(*conn_args, **conn_kwargs)
        else:
            raise Exception('unknown connector type')

    # source is object with __dict__ field or a module name (with 'in_module' flag up)
    @log_error
    def create_data(self, source=None, in_module=False):
        tables = self.meta['connector'].table_info()
        t = dict()

        for name, field in tables.items():
            t[name] = QRTable(name, field)

        self.__dict__.update(t)
        if source:
            if in_module:
                source = sys.modules[source]
            source.__dict__.update(t)

    @log_error
    def select(self, table: QRTable, *args: QRField):
        if len(args) == 0:
            fields = '*'
        else:
            fields = ('{},' * len(args))[:-1]

        request = 'select ' + fields + ' from {}'
        table_name = table.meta['table_name']
        identifiers = [f.name for f in args] + [table_name]

        return QRSelect(self.meta['connector'], table, request, identifiers)

    # todo add update, delete, insert