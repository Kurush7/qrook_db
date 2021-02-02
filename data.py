class QRTable:
    # fields = {'a':'int', 'b':'varchar'}
    def __init__(self, table_name=None, fields=None):
        self.meta = dict()
        self.meta['table_name'] = table_name
        self.meta['fields'] = {}

        if fields is None: return
        for name, value_type in fields:
            f = QRField(name, value_type, table_name)
            self.meta['fields'][name] = f
            self.__dict__[name] = f

    # todo add fields
    def __str__(self):
        if self.meta['table_name'] is None:
            return '<Empty QRTable>'
        return '<QRTable ' + self.meta['table_name'] + '>'

    # todo add select and etc


class QRField:
    def __init__(self, name, value_type, table_name):
        self.name = name
        self.type = value_type
        self.table_name = table_name

    def __str__(self):
        if self.name is None:
            return '<Empty QRField>'
        return '<QRField ' + self.name + ' of ' + self.table_name + '>'

class Data:
    pass
    # sys.modules[__name__]