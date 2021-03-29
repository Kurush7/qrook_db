import qrookDB.DB as qrdb
from data import QRTable
import unittest

# todo add tests for insert, update, delete, exec, function exec with returning

DB = qrdb.DB('postgres', 'qrookdb_test', 'kurush', 'pondoxo', format_type='dict')
op = DB.operators
DB.create_logger(app_name='qrookdb_test')
DB.create_data(__name__, in_module=True)

class TestBasic(unittest.TestCase):
    def test_tables_exist(self):
        self.assertTrue(isinstance(DB.a, QRTable))
        self.assertTrue(isinstance(DB.b, QRTable))
        self.assertTrue(isinstance(DB.ab, QRTable))

class TestRequest(unittest.TestCase):
    def assert_no_empty_data(self, data):
        self.assertTrue(data is not None and len(data) > 0)

    def assert_data_structure(self, data, type=None, rows_cnt=None, fields_cnt=None):
        if type:
            self.assertTrue(isinstance(data, type))
        if rows_cnt:
            self.assertTrue(data is not None and len(data) == rows_cnt)
        if fields_cnt:
            for d in data:
                self.assertTrue(len(d) == fields_cnt)

class TestSelect(TestRequest):
    def test_all(self):
        data1 = DB.select(DB.a).all()
        data2 = DB.a.select().all()
        self.assert_no_empty_data(data1)
        self.assert_no_empty_data(data2)

    def test_one(self):
        data1 = DB.select(DB.a).one()
        data2 = DB.b.select().one()
        self.assert_data_structure(data1, type=dict, rows_cnt=2)
        self.assert_data_structure(data2, type=dict, rows_cnt=2)

    def test_limit(self):
        data = DB.select(DB.a).limit(1).all()
        self.assert_data_structure(data, type=list, rows_cnt=1, fields_cnt=2)

    def test_offset(self):
        data = DB.select(DB.a, DB.a.id).order_by(DB.a.id).offset(5).all()
        for d in data:
            self.assertGreater(d['id'], 5)

    def test_order_by(self):
        data1 = DB.select(DB.a).order_by(DB.a.id).all()
        data2 = DB.select(DB.a).order_by(DB.a.id, desc=True).all()
        for i in range(1, len(data1)):
            self.assertGreater(data1[i]['id'], data1[i-1]['id'])
            self.assertLess(data2[i]['id'], data2[i-1]['id'])

    def test_distinct(self):
        data = DB.select(DB.ab, DB.ab.b_id, distinct=True).all()
        self.assert_data_structure(data, type=list, rows_cnt=3, fields_cnt=1)

    def test_add_attribute(self):
        query = DB.select(DB.a, DB.a.id)
        query.add_attribute(DB.a.name)
        data = query.all()
        self.assert_data_structure(data, type=list, fields_cnt=2)

    def test_group_by(self):
        data = DB.select(DB.a, 'count(*)').join(DB.ab, op.Eq(DB.a.id, DB.ab.a_id))\
            .join(DB.b, op.Eq(DB.b.id, DB.ab.b_id)).group_by(DB.b.info).all()

        data2 = DB.b.select().all()
        self.assert_data_structure(data, type=list, rows_cnt=len(data2), fields_cnt=1)
        pass

    def test_join(self):
        data = DB.select(DB.a, DB.a.id, DB.a.name, DB.b.id, DB.b.info).join(DB.ab, op.Eq(DB.a.id, DB.ab.a_id))\
            .join(DB.b, op.Eq(DB.b.id, DB.ab.b_id)).all()
        data2 = DB.ab.select().all()
        self.assert_data_structure(data, type=list, rows_cnt=len(data2), fields_cnt=4)

    def test_where(self):
        # todo test other ops
        data = DB.a.select().where(id=1).all()
        self.assert_data_structure(data, type=list, rows_cnt=1)

        data = DB.a.select().where(id=op.In(1,3,5,8)).all()
        self.assert_data_structure(data, type=list, rows_cnt=4)

        data = DB.a.select().where(id=1).where(id=2).all()
        self.assert_data_structure(data, type=list, rows_cnt=0)

        data = DB.a.select().where(id=1).where(id=2, bool='or').all()
        self.assert_data_structure(data, type=list, rows_cnt=2)

        data = DB.a.select().where(id=1).where(id=op.LE(10), name='Door', bool='or').all()
        self.assert_data_structure(data, type=list, rows_cnt=11)

    def test_errors(self):
        query = DB.a.select().where(id=1).where(id='a')
        data = query.all()
        self.assertEqual(data, None)
        self.assertEqual(query.get_error(), None)

        query = DB.a.select(DB.a.id, DB.a.id)
        data = query.all()
        self.assertEqual(data, None)
        self.assertTrue(isinstance(query.get_error(), str))

        query = DB.a.select(DB.a.id).where(id=1).add_attribute(DB.a.name)
        data = query.all()
        self.assertEqual(data, None)
        self.assertTrue(isinstance(query.get_error(), str))


# def main():
#
#     ok = DB.update(events, auto_commit=True).set(time=t).where(id=6).exec()
#     print(ok)
#
#     #ok = DB.insert(events, events.time, auto_commit=True).values([t]).exec()
#     #ok = DB.insert(events, events.date, events.time, auto_commit=True).values([d, t]).exec()
#     query = events.insert(events.date, events.time, auto_commit=False).values([[d, t], [None, t]]).returning('*')
#     data = query.all()
#     DB.commit()
#     print(data)
#
#     query = events.insert(events.date, events.time, auto_commit=True).values([[d, t], [None, t]]).returning(events.date, events.time)
#     data = query.all()
#     print(data)
#
#     query = events.insert(events.date, events.time, auto_commit=True).values([[d, t], [None, t]]).returning(['date', 'time'])
#     data = query.all()
#     print(data)
#
#     query = events.insert(events.date, events.time, auto_commit=True).values([[d, t], [None, t]]).returning('date', 'time')
#     data = query.all()
#     print(data)
#
#     data = DB.exec('select * from get_book_authors(1) as f(id int, name varchar)').config_fields('id', 'name').all()
#     print(data)
#
#     data = DB.funcall('get_book_authors', 1).config_fields('id', 'name').all()
#     print('funcall:', data)
#
#     q = DB.select(events, events.id, events.id).where(id=1)
#     data = q.all()
#     print('data:', data, ';\terror:', q.get_error())


if __name__ == '__main__':
    unittest.main()

sql = '''
create table A(id int primary key, name varchar);
create table B(id int primary key, info varchar);
create table AB(a_id int constraint a_fkey references A not null,
                b_id int constraint b_fkey references B not null);

insert into A values
(1, 'Kermit'), (2, 'Grover'),
(3, 'Сookie Monster'), (4, 'Beast'),
(5, 'Bert'), (6, 'Bertina'),
(7, 'Count von Count'), (8, 'Cranky'),
(9, 'Captain Cabbage'), (10, 'The Chief'),
(11, 'Doctor Two'), (12, 'Door'), (13, 'Elmo');

insert into B values
(1, 'fav character'), (2, 'odd character'), (3, 'aux character');

insert into AB values
(1, 2), (2, 2), (3, 1), (3, 2), (4, 1), (5, 3), (6, 3),
(7, 1), (8, 2), (9, 2), (10, 3), (11, 3),
(12, 3), (13, 2);
'''
