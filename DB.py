import PostgresConnector
from error_handlers import *
from data import *
from request import *
import sys

# todo add exec raw query
# todo add select(books.id) parsing -> extract table from ONLY datafield

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
            t[name] = QRTable(name, field, self)

        self.__dict__.update(t)
        if source:
            if in_module:
                source = sys.modules[source]
            source.__dict__.update(t)

    @log_error
    def commit(self):
        self.meta['connector'].commit()


    @log_error
    def select(self, table: QRTable, *args):
        identifiers, literals = [], []
        if len(args) == 0:
            fields = '*'
        else:
            fields = ''
            for arg in args:
                if isinstance(arg, QRField):
                    fields += '{}.{},'
                    identifiers.extend([arg.table_name, arg.name])
                else:
                    logger.warning('UNSAFE: executing raw select from table %s with args: %s', table, args)
                    fields += arg + ','
            fields = fields[:-1]

        request = 'select ' + fields + ' from {}'
        table_name = table.meta['table_name']
        identifiers += [table_name]

        return QRSelect(self.meta['connector'], table, request, identifiers, literals)

    @log_error
    def delete(self, table: QRTable, auto_commit=False):
        identifiers, literals = [], []

        request = 'delete from {}'
        table_name = table.meta['table_name']
        identifiers += [table_name]

        return QRWhere(self.meta['connector'], table, request, identifiers,
                       literals, auto_commit)

    @log_error
    def update(self, table: QRTable, auto_commit=False):
        identifiers, literals = [], []

        request = 'update {}'
        table_name = table.meta['table_name']
        identifiers += [table_name]

        return QRUpdate(self.meta['connector'], table, request, identifiers,
                             literals, auto_commit)

    @log_error
    def insert(self, table: QRTable, *args, auto_commit=False):
        identifiers, literals = [], []
        if len(args) == 0:
            fields = ''
        else:
            fields = ''
            for arg in args:
                if isinstance(arg, QRField):
                    fields += '{},'
                    identifiers.extend([arg.name])
                else:
                    logger.warning('UNSAFE: executing raw select from table %s with args: %s', table, args)
                    fields += arg + ','
            fields = fields[:-1]
            fields = '(' + fields + ')'

        request = 'insert into {} ' + fields + ' values '
        table_name = table.meta['table_name']
        identifiers = [table_name] + identifiers

        return QRInsert(self.meta['connector'], table, request, identifiers, literals, auto_commit)
