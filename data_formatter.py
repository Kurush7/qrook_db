# todo load from config data format -> refactor
from error_handlers import log_error


class DataFormatter:
    def __init__(self):
        self.format = 'list'

    # result type -> 'all', 'one'
    @log_error
    def format_data(self, data, used_fields, result_type):
        if result_type is None or data is None:
            return None

        if self.format == 'list':
            return data
        elif self.format == 'dict':
            res = []
            n = len(used_fields)
            if result_type == 'one':
                return {used_fields[i]: data[i] for i in range(n)}
            else:
                for d in data:
                    res.append({used_fields[i]: d[i] for i in range(n)})
                return res

    def set_list_format(self):
        self.format = 'list'

    def set_dict_format(self):
        self.format = 'dict'


defaultDataFormatter = DataFormatter()
