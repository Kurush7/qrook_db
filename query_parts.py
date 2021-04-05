from abc import ABCMeta, abstractmethod, abstractproperty
from data import *
import operators as op
from collections.abc import Iterable
from symbols import *


def get_field(field_name: str, tables) -> QRField:
    """
    :param field_name: field name
    :param tables: iterable of QRTables
    :return: QRField corresponding to given name or error,
    if name wasn't found or found several suitable fields
    """

    cnt = 0
    field = None
    for t in tables:
        if t.__dict__.get(field_name) is not None:
            if cnt:
                raise Exception('ambiguous attribute name %s' % field_name)
            field = t.__dict__[field_name]
    if field is None:
        raise Exception('attribute %s not found' % field_name)
    return field


def parse_request_args(tables, *args, disable_full_name=False, **kwargs):
    """
    :param tables: list of QRTable - tables used in query
    :param disable_full_name: if False, identifiers will be extended with table name: like 'id'->'table.id'
    :return: 3 lists: identifiers, literals, query parts (conditions)
    """
    identifiers = []
    literals = []
    conditions = []
    for field_name, condition in kwargs.items():
        if not isinstance(condition, op.IQROperator):
            condition = op.Eq(condition)

        condition, lits = condition.condition(disable_full_name)
        field = get_field(field_name, tables)

        if disable_full_name:
            identifiers.extend([field.name])
        else:
            identifiers.extend([field.table_name, field.name])
        literals.extend(lits)
        conditions.append(condition)

    for arg in args:
        # todo else warning of unknown
        if isinstance(arg, str):
            conditions.append(arg)
        elif isinstance(arg, op.Eq):
            if arg.has_both_args():

                def arg_parse(a):
                    if not isinstance(a, QRField):
                        literals.append(a)
                        return QRDB_LITERAL
                    else:
                        identifiers.extend([a.name] if disable_full_name else [a.table_name, a.name])
                        return QRDB_IDENTIFIER if disable_full_name else QRDB_IDENTIFIER + '.' + QRDB_IDENTIFIER

                if isinstance(arg.arg2, op.IQROperator):
                    if isinstance(arg.arg1, QRField):
                        condition, lits = arg.arg2.condition(disable_full_name)
                        literals.extend(lits)
                        arg_parse(arg.arg1)
                    elif isinstance(arg.arg1, str):
                        condition, lits = arg.arg2.condition(disable_full_name)
                        condition = condition[condition.find(' '):]  # remove identifier stuff
                        condition = arg.arg1 + condition
                        literals.extend(lits)
                    else:
                        raise Exception('unsupported op.Eq arguments:', arg.arg1, arg.arg2)

                else:
                    condition = arg_parse(arg.arg1) + ' = ' + arg_parse(arg.arg2)

                conditions.append(condition)
    return identifiers, literals, conditions


class QueryPartData:
    """structured information for IQueryPart accumulated results"""
    def __init__(self, identifiers, literals, query: str):
        self.identifiers = identifiers
        self.literals = literals
        self.query = query


class IQueryPart:
    """
    Abstract class for db-query parts like group_by, where, limit etc.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_data(self, *args, **kwargs) -> QueryPartData:
        """use query part with given arguments; example: obj.where(id=1, name=In(['a','b']))"""

    @abstractmethod
    def get_name(self):
        """get name of query part - the one is used as a method name in QRQuery, so it must be a valid identifier"""

    @abstractmethod
    def get_data(self) -> QueryPartData:
        """get identifiers, literals and query string accumulated in query part (multiple calls for single
        IQueryPart object is possible)"""

    @abstractmethod
    def add_tables(self, tables):
        """add list of tables"""

    @abstractmethod
    def set_tables(self, tables):
        """set list of tables to use"""


class QueryPart(IQueryPart):
    def __init__(self, name: str, command: str = None, tables=None):
        if tables is None:
            tables = []
        if command is None:
            command = name
        self.tables = tables
        self.command = command
        self.identifiers, self.literals = [], []
        self.query = None
        self.name = name

    def add_conditions(self, conditions, joint=' '):
        for cond in conditions:
            if self.query is None:
                self.query = self.command + ' ' + cond
            else:
                self.query += ' ' + joint + ' ' + cond

    def get_data(self):
        return QueryPartData(self.identifiers, self.literals, self.query if self.query else '')

    def get_name(self):
        return self.name

    def add_data(self, qr_joint, *args, **kwargs):
        identifiers, literals, conditions = parse_request_args(self.tables, *args, **kwargs)
        self.identifiers.extend(identifiers)
        self.literals.extend(literals)

        self.add_conditions(conditions, joint=qr_joint)

    def add_tables(self, *tables):
        self.tables += tables

    def set_tables(self, tables):
        self.tables = tables


class QRWhere(QueryPart):
    def __init__(self, tables=None):
        super().__init__('where', tables=tables)

    def add_data(self, *args, **kwargs):
        op = kwargs.get('bool')
        if op is not None:
            kwargs.pop('bool')
        if op not in ['and', 'or']:
            op = 'and'

        super().add_data(op, *args, **kwargs)


class QRGroupBy(QueryPart):
    def __init__(self, tables=None):
        super().__init__(name='group_by', command='group by', tables=tables)

    def add_data(self, *args, **kwargs):
        for field in args:
            self.identifiers.append(field.name)
        self.add_conditions([QRDB_IDENTIFIER] * len(args), joint=',')


class QRHaving(QueryPart):
    def __init__(self, tables=None):
        super().__init__('having', tables=tables)

    def add_data(self, *args, **kwargs):
        super().add_data(',', *args, **kwargs)


class QROrderBy(QueryPart):
    def __init__(self, tables=None):
        super().__init__(name='order_by', command='order by', tables=tables)

    def add_data(self, *args, **kwargs):
        sort_type = 'asc'
        if kwargs.get('desc') == True:
            sort_type = 'desc'

        for field in args:
            self.identifiers.append(field.name)

        self.add_conditions([QRDB_IDENTIFIER + ' ' + sort_type] * len(args), joint=',')


class QRLimit(QueryPart):
    def __init__(self, tables=None):
        super().__init__('limit', tables=tables)

    def add_data(self, *args, **kwargs):
        if len(kwargs) > 0 or len(args) != 1:
            raise Exception('Limit condition error: only one integer expected')
        n = args[0]
        if n != 'all' and not isinstance(n, int):
            raise Exception('Integer expected as a limit condition param')

        self.literals.append(n)
        self.query = ' limit ' + QRDB_LITERAL + ' '


class QROffset(QueryPart):
    def __init__(self, tables=None):
        super().__init__('offset', tables=tables)

    def add_data(self, *args, **kwargs):
        if len(kwargs) > 0 or len(args) != 1:
            raise Exception('Offset condition error: only one integer expected')
        n = args[0]
        if not isinstance(n, int):
            raise Exception('Offset expected as a limit condition param')

        self.literals.append(n)
        self.query = ' offset ' + QRDB_LITERAL + ' '


class QRJoin(QueryPart):
    def __init__(self, tables=None):
        super().__init__('join', tables=tables)

    def add_data(self, *args, **kwargs):
        if len(kwargs) > 0 or len(args) != 2:
            raise Exception('Join condition error: two conditional params')
        table, cond = args[0], args[1]
        self.add_tables(table)
        # todo be accurate with adding - links working for all query-parts!

        if type(cond) == str:
            self.identifiers.append(table.meta['table_name'])
            join_cond = QRDB_IDENTIFIER + ' on %s' % cond

        else:
            if not cond.has_both_args():
                raise Exception('join: op.Eq contains only one instance')
            if not isinstance(cond.arg1, QRField) or not isinstance(cond.arg2, QRField):
                raise Exception('join: op.Eq operands must be QRField instances')
            if cond.arg1.table not in self.tables or cond.arg1.table not in self.tables:
                raise Exception('join: wrong attribute\'s relations')

            self.identifiers.extend([table.meta['table_name'],
                                     cond.arg1.table_name, cond.arg1.name,
                                     cond.arg2.table_name, cond.arg2.name])
            join_cond = '%s on %s.%s = %s.%s' % (QRDB_IDENTIFIER, QRDB_IDENTIFIER,
                                                 QRDB_IDENTIFIER, QRDB_IDENTIFIER, QRDB_IDENTIFIER)

        self.add_conditions([join_cond], joint='join')


class QRSet(QueryPart):
    def __init__(self, tables=None):
        super().__init__('set', tables=tables)

    def add_data(self, *args, **kwargs):
        # todo here stuff like x>10 possible... in set - no sense
        identifiers, literals, conditions = parse_request_args(self.tables,
                                                               *args, **kwargs, disable_full_name=True)
        self.identifiers.extend(identifiers)
        self.literals.extend(literals)

        self.add_conditions(conditions, joint=',')


class QRValues(QueryPart):
    def __init__(self, column_cnt, tables=None):
        super().__init__('values', tables=tables)
        self.column_cnt = column_cnt

    def add_data(self, *args, **kwargs):
        literals = []
        value_sets = 0

        for arg in args:
            if not isinstance(arg, Iterable):
                raise Exception('insert values: iterable expected')
            flag = False
            for a in arg:
                if isinstance(a, Iterable) and not isinstance(a, str):
                    if len(a) != self.column_cnt:
                        raise Exception('insert values: wrong column count')
                    literals.extend(a)
                    value_sets += 1
                else:
                    literals.append(a)
                    flag = True
            if flag:
                value_sets += 1

        single = (QRDB_LITERAL + ',') * self.column_cnt
        single = '(' + single[:-1] + ')'
        query = ', '.join([single] * value_sets)

        if self.query is None:
            self.query = query
        else:
            self.query += ', ' + query
        self.literals.extend(literals)


class QRReturning(QueryPart):
    def __init__(self, used_fields, tables=None):
        super().__init__('returning', tables=tables)
        self.column_cnt = len(self.identifiers) - 1
        self.used_fields = used_fields

    def add_data(self, *args, **kwargs):
        identifiers = []
        query = []

        def parse_arg(arg, query):
            if isinstance(arg, QRField):
                query += [QRDB_IDENTIFIER]
                identifiers.extend([arg.name])
                self.used_fields.append(arg.name)
            else:
                if arg == '*':
                    self.used_fields.extend(list(self.tables[0].meta['fields'].keys()))
                else:
                    self.used_fields.append(arg)
                query += [arg]

        for arg in args:
            if isinstance(arg, Iterable) and not isinstance(arg, str):
                for a in arg:
                    parse_arg(a, query)
            else:
                parse_arg(arg, query)


        self.identifiers.extend(identifiers)
        self.add_conditions(query, joint=',')