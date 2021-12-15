from typing import List, Dict

from error_handlers import *

@log_class(log_error)
@exc_no_db(exceptions=['set_DB'])
class QRTable:
    """
    Object representing a table in database.
    Contains info about table (name, fields as QRField objects)
    and functions for quick access to queries
    """

    def __init__(self, table_name=None, fields=None, pk=None, DB=None):
        """
        :param fields: in format {'field_name': 'field_type', ...}
        :param DB: DBQueryAggregator instance
        """
        self.meta = dict()
        self.meta['table_name'] = table_name
        self.meta['fields'] = {}
        self.meta['primary_key'] = None
        self.meta['foreign_keys'] = []
        self._DB = DB

        if fields is None:
            return
        for name, value_type in fields:
            f = QRField(name, value_type, self, primary_key=name == pk,
                        foreign_key=None)
            if name == pk:
                self.meta['primary_key'] = f
            self.meta['fields'][name] = f
            self.__dict__[name] = f

    def add_foreign_keys(self, fks: List, tables: Dict):
        """
        :param tables - dict string:QRTable (string is table name)
        """
        for fk in fks:
            f = tables[fk['foreign_table']].meta['fields'][fk['foreign_column']]
            self.meta['fields'][fk['column']].set_foreign_key(f)
            self.meta['foreign_keys'].append(self.meta['fields'][fk['column']])

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

    def __init__(self, name, value_type, table: QRTable, primary_key: bool = False, foreign_key=None):
        """
        :param foreign_key - QRField instance
        """
        self.name = name
        self.type = value_type
        self.table_name = table.meta['table_name']
        self.table = table
        self.primary_key = primary_key
        self.foreign_key = foreign_key

    def set_foreign_key(self, fk):
        """
        :param fk - QRField instance
        """
        self.foreign_key = fk


    def __str__(self):
        if self.name is None:
            return '<Empty QRField>'
        return '<QRField ' + self.name + ' of ' + self.table_name + '>'