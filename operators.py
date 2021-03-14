from abc import ABCMeta, abstractmethod, abstractproperty
from symbols import QRDB_IDENTIFIER, QRDB_LITERAL

class IQROperator:
    """
    Abstract class representing sql operator.
    The only purpose is to create the condition string and literals literals for query.
    All operators are supposed to work like this
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def condition(self, short_name=False):
        """
        :param short_name: whether to use a short name or a full one (with table name or not)
        :return: pair: <request_string>, <literals array>
        """


class QROperator(IQROperator):
    def __init__(self, op, literals):
        self._literals = literals
        self._op = op

    @staticmethod
    def _get_name(short_name):
        return QRDB_IDENTIFIER + ' ' if short_name else '%s.%s ' % (QRDB_IDENTIFIER, QRDB_IDENTIFIER)

    def condition(self, short_name=False):
        return self._get_name(short_name) + self._op + QRDB_LITERAL, [self._literals]


class Between(QROperator):
    """
    between operator
    """
    def __init__(self, a, b):
        super().__init__('between', [a, b])

    def condition(self, short_name=False):
        return self._get_name(short_name) + ' between %s and %s' % (QRDB_LITERAL, QRDB_LITERAL), \
               list(self._literals)


class In(QROperator):
    """
    in operator
    """
    def __init__(self, *args):
        """
        :param args: iterable of elements (possible to be casted to a list)
        """
        super().__init__('in', args)

    def condition(self, short_name=False):
        ins = ','.join([QRDB_LITERAL] * len(self._literals))
        return super()._get_name(short_name) + ' in(' + ins + ')', list(self._literals)


class Eq(QROperator):
    """
    equal operator
    todo not very beautiful
    """
    def __init__(self, arg1, arg2=None):
        """
        :param arg1 - literal (or identifier, if arg2 set), right side of '='
        :param arg2: optional. if set, both args are identifiers' names
        examples of 'condition' call:
        Eq(16).condition() -> '{id}={literal}', [2000]
        Eq('a_id', 'b_id') -> '{id}={id}', []
        """
        self.arg1, self.arg2, self.duo = arg1, arg2, arg2 is not None
        super().__init__('=', arg1)

    def has_both_args(self):
        return self.duo

    def condition(self, short_name=False):
        if not self.duo:
            return super().condition(short_name)
        else:
            return self._get_name(short_name) + ' = ' + self._get_name(short_name), []


class GT(QROperator):
    """
    greater operator
    """
    def __init__(self, literals):
        super().__init__('>', literals)


class GE(QROperator):
    """
    greater or equal operator
    """
    def __init__(self, literals):
        super().__init__('>=', literals)


class LT(QROperator):
    """
    less operator
    """
    def __init__(self, literals):
        super().__init__('<', literals)


class LE(QROperator):
    """
    less or equal operator
    """
    def __init__(self, literals):
        super().__init__('<=', literals)


class NE(QROperator):
    """
    not equal operator
    """
    def __init__(self, literals):
        super().__init__('<>', literals)


class Like(QROperator):
    """
    like operator
    """
    def __init__(self, literals):
        super().__init__(' like ', literals)
    # todo class like - update condition() method