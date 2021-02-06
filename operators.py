from data import QRField


class QROperator:
    data = None
    op = None

    def __init__(self, op, data):
        self.data = data
        self.op = op

    def condition(self):
        return '{}' + self.op + '%s', self.data

class Eq(QROperator):
    def __init__(self, data):
        super().__init__('=', data)

# class like - update condition() method