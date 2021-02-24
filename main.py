import DB as db
import operators as op
from data import QRTable
books, books_authors, authors, events = [QRTable()] * 4

# todo add 'as' syntax: 'select count(*) as cnt
# todo when dict format used, manage same names in different tables

def main():
    DB = db.DB('postgres', 'qrook_db', 'kurush', 'pondoxo', format='dict')
    DB.create_data(__name__, in_module=True)
    print(books, books.id)
    data = DB.select(books).where(original_publication_year=2000, language_code='eng').\
        where(id=op.In(470, 490, 485)).all()
    print(data[:10])

    data = books.select('count(*)').group_by(books.original_publication_year).all()
    print(data[:10])

    data = DB.select(books, books.id).where('id < 10').order_by(books.id, desc=True).\
        limit(3).offset(2).all()
    print('limit & offset', data)

    data = books.select(authors.id, books.id)\
        .join(books_authors, 'books_authors.book_id = books.id')\
        .join(authors, op.Eq(books_authors.author_id, authors.id)).all()
    print(data[:10])

    data = books.select(books.id).where(id=1).where(bool='or', id=2).all()
    print('with or:', data)

    #ok = DB.delete(events, auto_commit=True).exec()
    #print(ok)

    from datetime import datetime
    t = datetime.now().time()
    d = datetime.now().date()
    ok = DB.update(events, auto_commit=True).set(time=t).where(id=6).exec()
    print(ok)

    #ok = DB.insert(events, events.time, auto_commit=True).values([t]).exec()
    #ok = DB.insert(events, events.date, events.time, auto_commit=True).values([d, t]).exec()
    query = events.insert(events.date, events.time, auto_commit=True).values([[d, t], [None, t]]).returning('*')
    data = query.all()
    print(data)

    data = DB.exec('select * from get_book_authors(1) as f(id int, name varchar)').config_fields('id', 'name').all()
    print(data)


if __name__ == '__main__':
    main()