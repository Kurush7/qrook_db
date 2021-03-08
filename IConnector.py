from abc import ABCMeta, abstractmethod, abstractproperty


class IConnector:
    """
    Abstract class for db-connections
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def exec(self, query_str: str, identifiers=None, literals=None, result='all'):
        """
        :param query_str: request string, containing {} for identifiers and %s for literals
        :param identifiers: iterable of identifiers
        :param literals: iterable of literals
        :param result: one of 'all' and 'one' - amount of rows to return from query results
        :return: for result='all': [(1, 2, 3), ...] or []
                 for result='one': (1,2,3) or None
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
