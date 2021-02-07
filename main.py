import db.DB as db
import db.operators as op
from db.data import QRTable
books, books_authors, authors, events = [QRTable()] * 4


def main():
    DB = db.DB('postgres', 'qrook_db', 'kurush', 'pondoxo')
    DB.create_data(__name__, in_module=True)
    print(books, books.id)
    data = DB.select(books).where(original_publication_year=2000, language_code='eng').\
        where(id=op.In(470, 490, 485)).all()
    print(data[:10])

    data = books.select('count(*)').group_by(books.original_publication_year).all()
    print(data[:10])

    data = DB.select(books, books.id).where('id < 10').order_by(books.id, desc=True).one()
    print(data)

    data = books.select(authors.id, books.id)\
        .join(books_authors, 'books_authors.book_id = books.id')\
        .join(authors, op.Eq(books_authors.author_id, authors.id)).all()
    print(data[:10])

    #ok = DB.delete(events, auto_commit=True).exec()
    #print(ok)

    from datetime import datetime
    t = datetime.now().time()
    d = datetime.now().date()
    ok = DB.update(events, auto_commit=True).set(time=t).where(id=6).exec()
    print(ok)

    #ok = DB.insert(events, events.time, auto_commit=True).values([t]).exec()
    #ok = DB.insert(events, events.date, events.time, auto_commit=True).values([d, t]).exec()
    query = events.insert(events.date, events.time, auto_commit=True).values([[d, t], [None, t]])
    ok = query.exec()
    print(ok)


if __name__ == '__main__':
    main()