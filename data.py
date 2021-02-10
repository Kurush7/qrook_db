from error_handlers import *


@log_class(log_error)
class QRTable:
    # fields = {'a':'int', 'b':'varchar'}
    def __init__(self, table_name=None, fields=None, DB=None):
        self.meta = dict()
        self.meta['table_name'] = table_name
        self.meta['fields'] = {}
        self.DB = DB

        if fields is None: return
        for name, value_type in fields:
            f = QRField(name, value_type, self)
            self.meta['fields'][name] = f
            self.__dict__[name] = f

    def __str__(self):
        if self.meta['table_name'] is None:
            return '<Empty QRTable>'
        return '<QRTable ' + self.meta['table_name'] + '>'

    def select(self, *args, **kwargs):
        if self.DB is None:
            raise Exception('no DB instance set for table to make queries to')
        return self.DB.select(self, *args, **kwargs)

    def update(self, *args, **kwargs):
        if self.DB is None:
            raise Exception('no DB instance set for table to make queries to')
        return self.DB.update(self, *args, **kwargs)

    def insert(self, *args, **kwargs):
        if self.DB is None:
            raise Exception('no DB instance set for table to make queries to')
        return self.DB.insert(self, *args, **kwargs)

    def delete(self, *args, **kwargs):
        if self.DB is None:
            raise Exception('no DB instance set for table to make queries to')
        return self.DB.delete(self, *args, **kwargs)


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
