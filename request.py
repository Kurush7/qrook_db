from data_formatter import format_data
import db.operators as op
from db.data import *
from error_handlers import *
from connectors.Connector import Connector
# accurate - db.operators needed with 'db.' to avoid different namespaces-> isinstance will fail
from collections.abc import Iterable


# todo unsafe warnings - deal with security on raw strings


def request_operators_order(op):
    if op == 'join': return 0
    if op == 'set': return 0
    if op == 'values':
        return 0
    elif op == 'where':
        return 1
    elif op == 'group_by':
        return 2
    elif op == 'having':
        return 3
    elif op == 'order_by':
        return 4


def get_field(field_name, tables) -> QRField:
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
    identifiers = []
    literals = []
    conditions = []
    for field_name, condition in kwargs.items():
        if not isinstance(condition, op.QROperator):
            condition = op.Eq(condition)

        condition, lits = condition.condition(disable_full_name)
        field = get_field(field_name, tables)

        if disable_full_name:
            identifiers.extend([field.name])
        else:
            identifiers.extend([field.table_name, field.name])
        literals.extend(lits)
        conditions.append(condition)

    if args:
        logger.warning("UNSAFE: executing raw select from table %s:  'where %s'", tables[0], args)
        for arg in args:
            conditions.append(arg)

    return identifiers, literals, conditions


class QRequest:
    def __init__(self, connector: Connector, table: QRTable, request: str = '',
                 identifiers=None, literals=None, auto_commit=False):
        self.connector = connector
        self.request = request
        if identifiers is None:
            identifiers = []
        if literals is None:
            literals = []

        self.identifiers = identifiers
        self.literals = literals
        self.tables = [table]
        self.conditions = dict()
        self.auto_commit = auto_commit
        self.cur_order = -1

    @log_error
    def exec(self, result=None):
        cond_ops = list(self.conditions.keys())
        cond_ops.sort(key=lambda x: request_operators_order(x))
        for ext in [self.conditions[i] for i in cond_ops]:
            self.request += ' ' + ext
        self.connector.exec(self.request, self.identifiers, self.literals, result=result)

        if self.auto_commit:
            self.connector.commit()
        return True


class QRWhere(QRequest):
    # result of base select
    def __init__(self, connector: Connector, table: QRTable, request: str = '',
                 identifiers=None, literals=None, auto_commit=False):
        super().__init__(connector, table, request, identifiers, literals, auto_commit)

    @log_error
    def where(self, *args, **kwargs):
        if request_operators_order('group_by') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, request_operators_order('group_by'))

        identifiers, literals, conditions = parse_request_args(self.tables, *args, **kwargs)
        self.identifiers.extend(identifiers)
        self.literals.extend(literals)

        for cond in conditions:
            if self.conditions.get('where') is None:
                self.conditions['where'] = ' where ' + cond
            else:
                self.conditions['where'] += ' and ' + cond

        return self


class QRSelect(QRWhere):
    def __init__(self, connector: Connector, table: QRTable, request: str = '',
                 identifiers=None, literals=None):
        super().__init__(connector, table, request, identifiers, literals, False)

    @log_error
    def exec(self, result=None):
        cond_ops = list(self.conditions.keys())
        cond_ops.sort(key=lambda x: request_operators_order(x))
        for ext in [self.conditions[i] for i in cond_ops]:
            self.request += ' ' + ext
        data = self.connector.exec(self.request, self.identifiers, self.literals, result=result)
        return format_data(data)

    @log_error
    def group_by(self, *args):
        if request_operators_order('group_by') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, request_operators_order('group_by'))

        for field in args:
            # todo check that field is in known tables
            self.identifiers.append(field.name)

            if self.conditions.get('group_by') is None:
                self.conditions['group_by'] = ' group by {}'
            else:
                self.conditions['group_by'] += ', {}'

        return self

    @log_error
    def having(self, *args, **kwargs):
        if request_operators_order('having') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, request_operators_order('having'))

        identifiers, literals, conditions = parse_request_args(self.tables, *args, **kwargs)
        self.identifiers.extend(identifiers)
        self.literals.extend(literals)

        for cond in conditions:
            if self.conditions.get('having') is None:
                self.conditions['having'] = ' having ' + cond
            else:
                self.conditions['having'] += ' having ' + cond
        return self

    @log_error
    def order_by(self, *args, **kwargs):
        if request_operators_order('order_by') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, request_operators_order('order_by'))

        sort_type = 'asc'
        if kwargs.get('desc') == True:
            sort_type = 'desc'

        for field in args:
            # todo check that field is in known tables
            self.identifiers.append(field.name)

            if self.conditions.get('order_by') is None:
                self.conditions['order_by'] = ' order by {} ' + sort_type
            else:
                self.conditions['order_by'] += ', {} ' + sort_type

        return self

    @log_error
    def join(self, table: QRTable, cond):
        if request_operators_order('join') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, request_operators_order('join'))

        self.tables.append(table)

        if type(cond) == str:
            logger.warning("UNSAFE: executing raw select from table %s:  'join on %s'",
                           self.tables[0], cond)
            self.identifiers.append(table.meta['table_name'])
            join_cond = 'join {} on %s' % cond

        else:
            if not isinstance(cond, op.Eq):
                raise Exception('join: op.Eq instance expected, got %s' % type(cond))
            if not cond.duos:
                raise Exception('join: op.Eq contains only one instance')
            if not isinstance(cond.arg1, QRField) or not isinstance(cond.arg2, QRField):
                raise Exception('join: op.Eq operands must be QRField instances')
            if cond.arg1.table not in self.tables or cond.arg1.table not in self.tables:
                raise Exception('join: wrong attribute\'s relations')

            self.identifiers.extend([table.meta['table_name'],
                                     cond.arg1.table_name, cond.arg1.name,
                                     cond.arg2.table_name, cond.arg2.name])
            join_cond = ' join {} on {}.{} = {}.{}'

        if self.conditions.get('join') is None:
            self.conditions['join'] = ''
        self.conditions['join'] += join_cond
        return self

    def all(self):
        return self.exec('all')

    def one(self):
        return self.exec('one')


class QRUpdate(QRWhere):
    def __init__(self, connector: Connector, table: QRTable, request: str = '',
                 identifiers=None, literals=None, auto_commit=False):
        super().__init__(connector, table, request, identifiers, literals, auto_commit)

    @log_error
    def set(self, *args, **kwargs):
        if request_operators_order('set') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, request_operators_order('set'))

        # todo here stuff like x>10 possible... in set - no sense
        identifiers, literals, conditions = parse_request_args(self.tables, *args, **kwargs,
                                                               disable_full_name=True)
        self.identifiers.extend(identifiers)
        self.literals.extend(literals)

        for cond in conditions:
            if self.conditions.get('set') is None:
                self.conditions['set'] = ' set ' + cond
            else:
                self.conditions['set'] += ', ' + cond

        return self


class QRInsert(QRequest):
    def __init__(self, connector: Connector, table: QRTable, request: str = '',
                 identifiers=None, literals=None, auto_commit=False):
        super().__init__(connector, table, request, identifiers, literals, auto_commit)
        self.column_cnt = len(identifiers) - 1
        self.data_query = ''

    @log_error
    def exec(self, result=None):
        self.request += ' ' + self.data_query
        self.connector.exec(self.request, self.identifiers, self.literals, result=result)
        if self.auto_commit:
            self.connector.commit()
        return True

    @log_error
    def values(self, *args):
        if request_operators_order('values') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, request_operators_order('values'))
        literals = []
        value_sets = 0

        for arg in args:
            if not isinstance(arg, Iterable):
                raise Exception('insert values: iterable expected')
            flag = False
            for a in arg:
                if isinstance(a, Iterable):
                    if len(a) != self.column_cnt:
                        raise Exception('insert values: wrong column count')
                    literals.extend(a)
                    value_sets += 1
                else:
                    literals.append(a)
                    flag = True
            if flag:
                value_sets += 1

        single = '%s,' * self.column_cnt
        single = '(' + single[:-1] + ')'
        query = ', '.join([single] * value_sets)

        if len(self.data_query) == 0:
            self.data_query = query
        else:
            self.data_query += ', ' + query
        self.literals.extend(literals)
        return self