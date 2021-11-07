from error_handlers import *

@log_class(log_error)
@exc_no_db(exceptions=['set_DB'])
class QRTable:
    """
    Object representing a table in database.
    Contains info about table (name, fields as QRField objects)
    and functions for quick access to queries
    """

    def __init__(self, table_name=None, fields=None, DB=None):
        """
        :param fields: in format {'field_name': 'field_type', ...}
        :param DB: DBQueryAggregator instance
        """
        self.meta = dict()
        self.meta['table_name'] = table_name
        self.meta['fields'] = {}
        self._DB = DB

        if fields is None: return
        for name, value_type in fields:
            f = QRField(name, value_type, self)
            self.meta['fields'][name] = f
            self.__dict__[name] = f

    def __str__(self):
        if self.meta['table_name'] is None:
            return '<Empty QRTable>'
        return '<QRTable ' + self.meta['table_name'] + '>'

    def set_DB(self, DB):
        self._DB = DB

    def select(self, *args, **kwargs):
        return self._DB.select(self, *args, **kwargs)

    def update(self, *args, **kwargs):
        return self._DB.update(self, *args, **kwargs)

    def insert(self, *args, **kwargs):
        return self._DB.insert(self, *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._DB.delete(self, *args, **kwargs)


class QRField:
    """
    Object representing a table's field in database.
    """

    def __init__(self, name, value_type, table: QRTable):
        self.name = name
        self.type = value_type
        self.table_name = table.meta['table_name']
        self.table = table

    def __str__(self):
        if self.name is None:
            return '<Empty QRField>'
        return '<QRField ' + self.name + ' of ' + self.table_name + '>'
