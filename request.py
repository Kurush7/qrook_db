from data_formatter import format_data
import db.operators as op
from db.data import *

# todo accurate - db.operators needed with 'db.' to avoid different namespaces-> isinstance will fail
# todo where & having & orderby... all of them: from the left not only identifier: having count(*) > 5, for example
# todo copy-paste where and having
# todo add join

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

# todo make heir from qrequest
# todo add logging errors
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

    def where(self, **kwargs):
        if select_order('where') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, select_order('where'))

        for field_name, condition in kwargs.items():
            if not isinstance(condition, op.QROperator):
                condition = op.Eq(condition)

            condition, literals = condition.condition()
            field = get_field(field_name, self.tables)

            self.identifiers.append(field.name)
            self.literals.extend(literals)

            if self.conditions.get('where') is None:
                self.conditions['where'] = ' where ' + condition
            else:
                self.conditions['where'] += ' and ' + condition

        return self

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

    def having(self, **kwargs):
        if select_order('having') < self.cur_order:
            raise Exception('select: wrong operators sequence')
        self.cur_order = max(self.cur_order, select_order('having'))

        for field_name, condition in kwargs.items():
            if not isinstance(condition, op.QROperator):
                condition = op.Eq(condition)

            condition, literals = condition.condition()
            field = get_field(field_name, self.tables)

            self.identifiers.append(field.name)
            self.literals.extend(literals)

            if self.conditions.get('having') is None:
                self.conditions['having'] = ' having ' + condition
            else:
                self.conditions['having'] += ' and ' + condition

        return self

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


    def all(self):
        return self.exec('all')

    def one(self):
        return self.exec('one')
