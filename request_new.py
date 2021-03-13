import inject
from abc import ABCMeta, abstractmethod, abstractproperty
from IConnector import IConnector
from data_formatter import IDataFormatter

from query_parts import *

# todo динамические методы - над ними нет декораторов

class IQRQuery:
    @abstractmethod
    def exec(self, result: str):
        """execute query"""

    @abstractmethod
    def all(self):
        """execute query"""

    @abstractmethod
    def one(self):
        """execute query"""

@log_class(log_error_default_self)
class QRQuery(IQRQuery):
    data_formatter = inject.attr(IDataFormatter)

    def __init__(self, connector: IConnector, table: QRTable = None, query: str = '',
                 identifiers=None, literals=None, used_fields=None, auto_commit=False):
        self.connector = connector
        self.query = query
        if identifiers is None:
            identifiers = []
        if literals is None:
            literals = []
        if used_fields is None:
            used_fields = []
        self.identifiers = identifiers
        self.literals = literals
        self.tables = [table] if table else []
        self.conditions = dict()
        self.auto_commit = auto_commit
        self.cur_order = -1
        self.used_fields = used_fields
        self.query_parts = []

    def __create_method(self, qp: IQueryPart):
        n = len(self.query_parts)

        def f(*args, **kwargs):
            if self.cur_order > n:
                raise Exception('select: wrong operators sequence')
            self.cur_order = n

            qp.add_data(*args, **kwargs)
            return self
        return f

    def _add_query_part(self, qp: IQueryPart):
        qp.set_tables(self.tables)
        method_name = qp.get_name()
        self.__dict__[method_name] = self.__create_method(qp)
        self.query_parts.append(qp)

    def exec(self, result=None):
        for qp in self.query_parts:
            data = qp.get_data()
            self.identifiers.extend(data.identifiers)
            self.literals.extend(data.literals)
            self.query += ' ' + data.query

        data = self.connector.exec(self.query, self.identifiers, self.literals, result=result)
        data.set_used_fields(self.used_fields)

        if self.auto_commit:
            self.connector.commit()

        return True if (result is None) else \
            self.data_formatter.format_data(data)

    def config_fields(self, *args):
        # todo add support for iterable params
        self.used_fields = list(args)
        return self

    def all(self):
        return self.exec('all')

    def one(self):
        return self.exec('one')


@log_class(log_error_default_self)
class QRSelect(QRQuery):
    def __init__(self, connector: IConnector, table: QRTable, *args):
        identifiers, literals, used_fields = [], [], []
        if len(args) == 0:
            own_args = list(table.meta['fields'].values())
        else:
            own_args = args

        fields = ''
        for arg in own_args:
            if isinstance(arg, QRField):
                fields += '{}.{},'
                identifiers.extend([arg.table_name, arg.name])
                used_fields.append(arg.name)
            else:
                fields += arg + ','
                used_fields.append(arg)
        fields = fields[:-1]

        query = 'select ' + fields + ' from {}'
        table_name = table.meta['table_name']
        identifiers += [table_name]

        super().__init__(connector, table, query=query,
                         identifiers=identifiers, literals=literals, used_fields=used_fields)
        self.configure_query_parts()

    def configure_query_parts(self):
        # todo order is important - must correlate with query parts order
        self._add_query_part(QRJoin(self.tables))
        self._add_query_part(QRWhere(self.tables))
        self._add_query_part(QRGroupBy(self.tables))
        self._add_query_part(QRHaving(self.tables))
        self._add_query_part(QROrderBy(self.tables))
        self._add_query_part(QRLimit(self.tables))
        self._add_query_part(QROffset(self.tables))


@log_class(log_error_default_self)
class QRUpdate(QRQuery):
    def __init__(self, connector: IConnector, table: QRTable, auto_commit: False):
        identifiers, literals = [], []

        query = 'update {}'
        table_name = table.meta['table_name']
        identifiers += [table_name]
        # todo manage used-fields

        super().__init__(connector, table, query=query,
                         identifiers=identifiers, literals=literals, auto_commit=auto_commit)
        self.configure_query_parts()

    def configure_query_parts(self):
        self._add_query_part(QRSet(self.tables))
        self._add_query_part(QRWhere(self.tables))
        # todo add returning... is it possible?


@log_class(log_error_default_self)
class QRDelete(QRQuery):
    def __init__(self, connector: IConnector, table: QRTable, auto_commit: False):
        identifiers, literals = [], []

        query = 'delete from {}'
        table_name = table.meta['table_name']
        identifiers += [table_name]

        super().__init__(connector, table, query=query,
                         identifiers=identifiers, literals=literals, auto_commit=auto_commit)
        self.configure_query_parts()

    def configure_query_parts(self):
        self._add_query_part(QRWhere(self.tables))
        # todo add returning... is it possible?

@log_class(log_error_default_self)
class QRInsert(QRQuery):
    def __init__(self, connector: IConnector, table: QRTable, *args, auto_commit=False):
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
                    fields += arg + ','
            fields = fields[:-1]
            fields = '(' + fields + ')'

        query = 'insert into {} ' + fields + ' values '
        table_name = table.meta['table_name']
        identifiers = [table_name] + identifiers

        super().__init__(connector, table, query=query,
                         identifiers=identifiers, literals=literals, auto_commit=auto_commit)
        self.configure_query_parts()

    def configure_query_parts(self):
        self._add_query_part(QRValues(tables=self.tables, column_cnt=len(self.identifiers) - 1))
        self._add_query_part(QRReturning(self.used_fields, self.tables))