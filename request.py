from data_formatter import format_data
from operators import *
import inspect

def select_order(op):
    if op == 'join': return 0
    elif op == 'where': return 1
    elif op == 'group by': return 2
    elif op == 'having': return 3
    elif op == 'order by': return 4


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
    def __init__(self, connector, table, request=None, identifiers=None):
        self.connector = connector
        self.request = request
        self.identifiers = identifiers
        self.literals = []
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
            if not inspect.isclass(condition):
                condition = Eq(condition)
            if not isinstance(condition, QROperator):
                raise Exception('where: QROperator is missing')

            condition, literals = condition.condition()
            field = get_field(field_name, self.tables)

            self.identifiers.append(field.name)
            self.literals.append(literals)

            if self.conditions.get('where') is None:
                self.conditions['where'] = ' where ' + condition
            else:
                self.conditions['where'] += ', ' + condition

        return self

    def all(self):
        return self.exec('all')

    def one(self):
        return self.exec('one')
