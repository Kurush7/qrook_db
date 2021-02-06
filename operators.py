from data import QRField


class QROperator:
    def __init__(self, op, data):
        self.data = data
        self.op = op

    def condition(self):
        return '{}' + self.op + '%s', [self.data]

class Between(QROperator):
    def __init__(self, a, b):
        super().__init__('<>', [a, b])

    def condition(self):
        return '{} between %s and %s', self.data

class In(QROperator):
    def __init__(self, *args):
        super().__init__('<>', args)

    def condition(self):
        likes = ','.join(['%s'] * len(self.data))
        return '{} in(' + likes + ')', self.data

class Eq(QROperator):
    def __init__(self, data):
        super().__init__('=', data)

class GT(QROperator):
    def __init__(self, data):
        super().__init__('>', data)

class GE(QROperator):
    def __init__(self, data):
        super().__init__('>=', data)

class LT(QROperator):
    def __init__(self, data):
        super().__init__('<', data)

class LE(QROperator):
    def __init__(self, data):
        super().__init__('<=', data)

class NE(QROperator):
    def __init__(self, data):
        super().__init__('<>', data)

class Like(QROperator):
    def __init__(self, data):
        super().__init__(' like ', data)



# class like - update condition() method