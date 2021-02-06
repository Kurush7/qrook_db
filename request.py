from data_formatter import format_data
import db.operators as op
from db.data import *
from error_handlers import *
# accurate - db.operators needed with 'db.' to avoid different namespaces-> isinstance will fail

# todo unsafe warnings - deal with security on raw strings
# todo manage many tables, ambiguous names


def select_order(op):
    if op == 'join': return 0
    elif op == 'where': return 1
    elif op == 'group_by': return 2
    elif op == 'having': return 3
    elif op == 'order_by': return 4


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


def parse_request_args(tables, *args, **kwargs):
    identifiers = []
    literals = []
    conditions = []
    for field_name, condition in kwargs.items():
        if not isinstance(condition, op.QROperator):
            condition = op.Eq(condition)

        condition, lits = condition.condition()
        field = get_field(field_name, tables)

        identifiers.extend([field.table_name, field.name])
        literals.extend(lits)
        conditions.append(condition)

    if args:
        logger.warning("UNSAFE: executing raw select from table %s:  'where %s'", tables[0], args)
        for arg in args:
            conditions.append(arg)

    return identifiers, literals, conditions


# todo make heir from qrequest
class QRSelect:
    connector = None
    request = None
    cur_order = -1

    # todo connector set to None, add 'setConn' method
    # result of base select
    def __init__(self, connector, table, request=None, identifiers=None, literals=None):
        if identifiers is None:
            identifiers = []
        if literals is None:
            literals = []

        self.connector = connector
        self.request = request
        self.identifiers = identifiers
        self.literals = literals
        self.tables = [table]

        self.conditions = dict()

    def exec(self, result=None):
        cond_ops = list(self.conditions.keys())
        cond_ops.sort(key=lambda x: select_order(x))
        for ext in [self.conditions[i] for i in cond_ops]:
            self.request += ' ' + ext
        data = self.connector.exec(self.request, self.identifiers, self.literals, result=result)
        return format_data(data)

    @log_error
    def where(self, *args, **kwargs):
        if select_order('where') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, select_order('where'))

        identifiers, literals, conditions = parse_request_args(self.tables, *args, **kwargs)
        self.identifiers.extend(identifiers)
        self.literals.extend(literals)

        for cond in conditions:
            if self.conditions.get('where') is None:
                self.conditions['where'] = ' where ' + cond
            else:
                self.conditions['where'] += ' and ' + cond

        return self

    @log_error
    def group_by(self, *args):
        if select_order('group_by') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, select_order('group_by'))

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
        if select_order('having') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, select_order('having'))

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
        if select_order('order_by') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, select_order('order_by'))

        sort_type = 'asc'
        if kwargs.get('desc') == True:
            sort_type = 'desc'

        for field in args:
            # todo check that field is in known tables
            # todo add asc and desc
            self.identifiers.append(field.name)

            if self.conditions.get('order_by') is None:
                self.conditions['order_by'] = ' order by {} ' + sort_type
            else:
                self.conditions['order_by'] += ', {} ' + sort_type

        return self

    @log_error
    def join(self, table: QRTable, cond):
        if select_order('join') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, select_order('join'))

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
