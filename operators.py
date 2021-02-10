from data import QRField


class QROperator:
    def __init__(self, op, data):
        self.data = data
        self.op = op

    def condition(self, disable_full_name=False):
        if disable_full_name:
            return '{}' + self.op + '%s', [self.data]
        else:
            return '{}.{}' + self.op + '%s', [self.data]

class Between(QROperator):
    def __init__(self, a, b):
        super().__init__('<>', [a, b])

    def condition(self, disable_full_name=False):
        if disable_full_name:
            return '{} between %s and %s', list(self.data)
        else:
            return '{}.{} between %s and %s', list(self.data)

class In(QROperator):
    def __init__(self, *args):
        super().__init__('<>', args)

    def condition(self, disable_full_name=False):
        likes = ','.join(['%s'] * len(self.data))
        if disable_full_name:
            return '{} in(' + likes + ')', list(self.data)
        else:
            return '{}.{} in(' + likes + ')', list(self.data)

class Eq(QROperator):
    def __init__(self, arg1, arg2=None):
        self.arg1 = arg1
        self.arg2 = arg2
        self.duos = arg2 is not None
        super().__init__('=', arg1)

    def condition(self, disable_full_name=False):
        if not self.duos:
            return super().condition(disable_full_name)
        else:
            if disable_full_name:
                return '{} = {}', []
            else:
                return '{}.{} = {}.{}', []

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