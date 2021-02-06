class QRTable:
    # fields = {'a':'int', 'b':'varchar'}
    def __init__(self, table_name=None, fields=None):
        self.meta = dict()
        self.meta['table_name'] = table_name
        self.meta['fields'] = {}

        if fields is None: return
        for name, value_type in fields:
            f = QRField(name, value_type, self)
            self.meta['fields'][name] = f
            self.__dict__[name] = f

    def __str__(self):
        if self.meta['table_name'] is None:
            return '<Empty QRTable>'
        return '<QRTable ' + self.meta['table_name'] + '>'

    # todo add select, insert etc as references to DB methods


class QRField:
    def __init__(self, name, value_type, table: QRTable):
        self.name = name
        self.type = value_type
        self.table_name = table.meta['table_name']
        self.table = table

    def __str__(self):
        if self.name is None:
            return '<Empty QRField>'
        return '<QRField ' + self.name + ' of ' + self.table_name + '>'