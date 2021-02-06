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
        return '{} between %s and %s', list(self.data)

class In(QROperator):
    def __init__(self, *args):
        super().__init__('<>', args)

    def condition(self):
        likes = ','.join(['%s'] * len(self.data))
        return '{} in(' + likes + ')', list(self.data)

class Eq(QROperator):
    def __init__(self, arg1, arg2=None):
        self.arg1 = arg1
        self.arg2 = arg2
        self.duos = arg2 is not None
        super().__init__('=', arg1)

    def condition(self):
        if not self.duos:
            return super().condition()
        else:
            return '{} = {}', []

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