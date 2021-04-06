from abc import ABCMeta, abstractmethod, abstractproperty


class IConnector:
    """
    Abstract class for db-connections
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def exec(self, query_str: str, identifiers=None, literals=None, result='all'):
        """
        :param query_str: request string,
            containing symbols.QRDB_IDENTIFIER for identifiers and symbols.QRDB_LITERAL for literals
        :param identifiers: iterable of identifiers
        :param literals: iterable of literals
        :param result: one of 'all' and 'one' - amount of rows to return from query results
        :return: DBResult instance
        """

    @abstractmethod
    def table_info(self):
        """
        return system info about tables in chosen db in given format:
        {'books':[('id', 'integer'), ('date', 'date'), ...], ...}
        """

    @abstractmethod
    def commit(self):
        """
        commit changes to database
        """


class DBResult:
    """
    class for db-query data.
    """

    def __init__(self, data, result_type):
        self.result_type = result_type
        self.data = data
        self.used_fields = []

    def set_used_fields(self, used_fields):
        self.used_fields = used_fields

    def get_data(self):
        return self.data

    def get_result_type(self):
        return self.result_type

    def get_used_fields(self):
        return self.used_fields

    def is_one_result(self):
        return self.result_type == 'one'

    def is_all_result(self):
        return self.result_type == 'all'

    def is_no_result(self):
        return self.result_type is None