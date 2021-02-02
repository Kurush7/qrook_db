import PostgresConnector
from error_handlers import *
from data import *
from request import *
import sys

class DB:
    @log_error
    def __init__(self, connector_type, *conn_args, **conn_kwargs):
        self.meta = dict()
        if connector_type == 'postgres':
            self.meta['connector'] = PostgresConnector.PostgresConnector(*conn_args, **conn_kwargs)
        else:
            raise Exception('unknown connector type')

    # todo interface: send __name__ as string and flag 'module_name'
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

    def select(self, table: QRTable, *args: QRField):
        if len(args) == 0:
            fields = '*'
        else:
            fields = ('{},' * len(args))[:-1]
        sql = 'select ' + fields + ' from ' + table.meta['table_name']
        return QRSelect(self.meta['connector'], sql, [f.name for f in args])