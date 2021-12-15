import qrookDB.DB as db
from data import QRTable
from qrookDB.SQLiteConnector import SQLiteConnector

sqlite_path = '../test_sqlite'

A, B = [QRTable] * 2
DB = db.DB(SQLiteConnector, sqlite_path, format_type='dict')
op = DB.operators
DB.create_logger(app_name='sqlite_test')
DB.create_data(__name__, in_module=True)


def main():
    print(DB.A)
    print(A, A.id)
    data = DB.select(A).all()
    print(data)


    query = A.insert(A.id, A.data, auto_commit=False).values([[10, 'a'], [20, 'b']])
    data = query.all()
    DB.commit()
    print(data)


if __name__ == '__main__':
    main()