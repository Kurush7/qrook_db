import os

import qrookDB.DB as db
from data import QRTable

books, books_authors, authors, events = [QRTable] * 4
DB = db.DB('postgres', 'qrook_db_new', 'kurush', 'pondoxo', format_type='dict')
op = DB.operators
DB.create_logger(app_name='qrookdb_test')
DB.create_data(__name__, in_module=True)


def print_tables():
    tables = DB.meta['tables']
    for table_name, table in tables.items():
        meta = table.meta
        print(f"\n===--- {meta['table_name']} ---===")
        for field_name, field in meta['fields'].items():
            pk_flag = field.primary_key is True  # or field == meta['primary_key']
            print(f"{'(*)' if pk_flag else '   '} {field.name}: {field.type}")

        if len(meta['foreign_keys']):
            print('Constraints:')
            for fields_with_fk in meta['foreign_keys']:
                fk = fields_with_fk.foreign_key
                print(f'\tForeign key: {fields_with_fk.name} references {fk.table.meta["table_name"]}({fk.name})')


if __name__ == '__main__':
    os.chdir('..')
    print_tables()