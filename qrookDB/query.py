import inject
from abc import ABCMeta, abstractmethod, abstractproperty
from IConnector import IConnector
from data_formatter import IDataFormatter

from query_parts import *
from symbols import *
import qrlogging


class IQRQuery:
    """
    Abstract class for db-queries like select, update, delete  etc.
    """

    @abstractmethod
    def exec(self, result: str = None):
        """
        execute the formed query
        :param result: one of 'all', 'one' - number of rows to get from query result
        :return: None if results is None, else data in format specified by injected IDataFormatter instance
        WARNING - if result is 'one', no 'limit 1' is added to query - one must specify that yourself
        """

    @abstractmethod
    def all(self):
        """shortcut for exec with result='all'"""

    @abstractmethod
    def one(self):
        """shortcut for exec with result='one'"""

    def get_error(self):
        """return None or str with description of error occured while making query"""


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
        self.error = None

    def get_error(self):
        return self.error

    def __create_method(self, qp: IQueryPart):
        n = len(self.query_parts)

        def f(*args, **kwargs):
            if self.error is not None:
                return self
            if self.cur_order > n:
                self.error = 'select: wrong operators sequence'
            self.cur_order = n

            try:
                qp.add_data(*args, **kwargs)
            except Exception as e:
                qrlogging.exception(e)
                self.error = str(e)
            return self
        return f

    def _add_query_part(self, qp: IQueryPart):
        qp.set_tables(self.tables)
        method_name = qp.get_name()
        self.__dict__[method_name] = self.__create_method(qp)
        self.query_parts.append(qp)

    @log_error_default()
    def exec(self, result=None):
        if self.error is not None:
            return None

        for qp in self.query_parts:
            data = qp.get_data()
            self.identifiers.extend(data.identifiers)
            self.literals.extend(data.literals)
            self.query += ' ' + data.query

        data = self.connector.exec(self.query, self.identifiers, self.literals, result=result)
        if data is not None:
            data.set_used_fields(self.used_fields)

        if self.auto_commit:
            self.connector.commit()

        return True if (result is None) else \
            self.data_formatter.format_data(data)

    def all(self):
        return self.exec('all')

    def one(self):
        return self.exec('one')


class ReturnConfigurator:
    """class for return fields configuration; must only be inherited with QRQuery class!"""
    def config_fields(self, *args):
        """set field names to return"""
        # todo add support for iterable params
        self.used_fields = list(args)
        return self

class QRSelect(QRQuery):
    def __init__(self, connector: IConnector, table: QRTable, *args, **kwargs):
        try:
            identifiers, literals, used_fields = [], [], dict()
            if len(args) == 0:
                own_args = list(table.meta['fields'].values())
            else:
                own_args = args

            fields = ''
            for arg in own_args:
                if isinstance(arg, QRField):
                    fields += '%s.%s,' % (QRDB_IDENTIFIER, QRDB_IDENTIFIER)
                    identifiers.extend([arg.table_name, arg.name])
                    self.__add_used_field(used_fields, {'name': arg.name, 'table': arg.table_name})
                elif isinstance(arg, str):  # todo else warning
                    fields += arg + ','
                    self.__add_used_field(used_fields, {'name': arg})
            fields = fields[:-1]

            ditinct = 'distinct' if kwargs.get('distinct') == True else ' '

            self.full_used_fields = used_fields
            used_fields = [k for k, v in used_fields.items() if not v.get('expired')]

            query = 'select ' + ditinct + fields + ' from ' + QRDB_IDENTIFIER
            table_name = table.meta['table_name']
            identifiers += [table_name]

            super().__init__(connector, table, query=query,
                             identifiers=identifiers, literals=literals, used_fields=used_fields)
        except Exception as e:
            super().__init__(connector, table)
            self.error = str(e)
            qrlogging.warning("Failed to init select-query: %s", e)
        finally:
            self.__configure_query_parts()

    def add_attribute(self, field: QRField):
        if self.error: return self
        if self.cur_order >= 0:
            self.error = 'can only add attributes strictly after query initializing'
            return self

        s = ',%s.%s ' % (QRDB_IDENTIFIER, QRDB_IDENTIFIER)
        i = self.query.find('from')
        self.query = self.query[:i] + s + self.query[i:]
        self.identifiers = self.identifiers[:-1] + [field.table_name, field.name] + [self.identifiers[-1]]
        new_name = self.__add_used_field(self.full_used_fields, {'name': field.name, 'table': field.table_name})
        self.used_fields.append(new_name)

    def __configure_query_parts(self):
        # order is important - must correlate with query parts order
        self._add_query_part(QRJoin(self.tables))
        self._add_query_part(QRWhere(self.tables))
        self._add_query_part(QRGroupBy(self.tables))
        self._add_query_part(QRHaving(self.tables))
        self._add_query_part(QROrderBy(self.tables))
        self._add_query_part(QRLimit(self.tables))
        self._add_query_part(QROffset(self.tables))

    def __add_used_field(self, fields, a):
        x = fields.get(a['name'])
        if x is not None:
            if x.get('table') is None or a.get('table') is None:
                raise Exception('two identical return fields set for select query')
            elif x.get('table') == a.get('table'):
                raise Exception('two identical return fields set for select query')
            elif x.get('expired') is None:
                fields[x['table'] + '_' + x['name']] = x.copy()
                fields[x['name']]['expired'] = True
                fields[x['name']]['tables'] = [x['table'], a['table']]
            else:
                if a['table'] in fields[a['name']]['tables']:
                    raise Exception('two identical return fields set for select query')
                fields[x['name']]['tables'].append(a['table'])
            fields[a['table'] + '_' + a['name']] = a
            return a['table'] + '_' + a['name']
        else:
            fields[a['name']] = a
            return a['name']


class QRUpdate(QRQuery):
    def __init__(self, connector: IConnector, table: QRTable, auto_commit=False):
        try:
            identifiers, literals = [], []

            query = 'update ' + QRDB_IDENTIFIER
            table_name = table.meta['table_name']
            identifiers += [table_name]

            super().__init__(connector, table, query=query,
                             identifiers=identifiers, literals=literals, auto_commit=auto_commit)
            self.__configure_query_parts()
        except Exception as e:
            super().__init__(connector, table)
            self.error = str(e)
            qrlogging.warning("Failed to init update-query: %s", e)
        finally:
            self.__configure_query_parts()

    def __configure_query_parts(self):
        self._add_query_part(QRSet(self.tables))
        self._add_query_part(QRWhere(self.tables))


class QRDelete(QRQuery):
    def __init__(self, connector: IConnector, table: QRTable, auto_commit=False):
        try:
            identifiers, literals = [], []

            query = 'delete from ' + QRDB_IDENTIFIER
            table_name = table.meta['table_name']
            identifiers += [table_name]

            super().__init__(connector, table, query=query,
                             identifiers=identifiers, literals=literals, auto_commit=auto_commit)
            self.__configure_query_parts()
        except Exception as e:
            super().__init__(connector, table)
            self.error = str(e)
            qrlogging.warning("Failed to init delete-query: %s", e)
        finally:
            self.__configure_query_parts()

    def __configure_query_parts(self):
        self._add_query_part(QRWhere(self.tables))


class QRInsert(QRQuery):
    def __init__(self, connector: IConnector, table: QRTable, *args, auto_commit=False):
        try:
            identifiers, literals = [], []
            if len(args) == 0:
                fields = ''
            else:
                fields = ''
                for arg in args:
                    if isinstance(arg, QRField):
                        fields += QRDB_IDENTIFIER + ' ,'
                        identifiers.extend([arg.name])
                    else:
                        fields += arg + ','
                fields = fields[:-1]
                fields = '(' + fields + ')'

            query = 'insert into ' + QRDB_IDENTIFIER + ' ' + fields + ' values '
            table_name = table.meta['table_name']
            identifiers = [table_name] + identifiers

            super().__init__(connector, table, query=query,
                             identifiers=identifiers, literals=literals, auto_commit=auto_commit)
            self.__configure_query_parts()
        except Exception as e:
            super().__init__(connector, table)
            self.error = str(e)
            qrlogging.warning("Failed to init insert-query: %s", e)
        finally:
            self.__configure_query_parts()

    def __configure_query_parts(self):
        self._add_query_part(QRValues(tables=self.tables, column_cnt=len(self.identifiers) - 1))
        self._add_query_part(QRReturning(self.used_fields, self.tables))


class QRExec(QRQuery, ReturnConfigurator):
    def __init__(self, connector: IConnector, raw_query, auto_commit=False):
        super().__init__(connector, query=raw_query, auto_commit=auto_commit)


class QRFuncCall(QRQuery, ReturnConfigurator):
    # todo not working: psycopg2 requires definitions for "record" return type
    def __init__(self, connector: IConnector, function: str, *args, auto_commit=False):
        try:
            params = [QRDB_LITERAL] * len(args)
            params = '(' + ', '.join(params) + ')'
            query = 'select * from ' + QRDB_IDENTIFIER + params
            identifiers = [function]
            literals = list(args)
            super().__init__(connector, query=query,
                             identifiers=identifiers, literals=literals, auto_commit=auto_commit)
        except Exception as e:
            super().__init__(connector)
            self.error = str(e)
            qrlogging.warning("Failed to init function-call-query: %s", e)
        finally:
            self.__configure_query_parts()

    def __configure_query_parts(self):
        self._add_query_part(QRReturning(self.used_fields, self.tables))

