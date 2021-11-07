from abc import ABCMeta, abstractmethod, abstractproperty
from error_handlers import log_error
from IConnector import DBResult


class IDataFormatter:
    """
    Abstract class for db-connections
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def format_data(self, data: DBResult):
        """
        :param query_str: request string, containing {} for identifiers and %s for literals
        :param identifiers: iterable of identifiers
        :param literals: iterable of literals
        :param result: one of 'all' and 'one' - amount of rows to return from query results
        :return: for result='all': [(1, 2, 3), ...] or []
                 for result='one': (1,2,3) or None
        """


class ListDataFormatter(IDataFormatter):
    def format_data(self, data: DBResult):
        return data.get_data()


class DictDataFormatter(IDataFormatter):
    def format_data(self, data: DBResult):
        if data.is_no_result():
            return None

        used_fields = data.get_used_fields()
        dt = data.get_data()
        res = []
        n = len(used_fields)
        if data.is_one_result():
            return {used_fields[i]: dt[i] for i in range(n)}
        elif data.is_all_result():
            for d in dt:
                res.append({used_fields[i]: d[i] for i in range(n)})
            return res
        else:
            raise Exception("unexpected data result: neither one nor all")