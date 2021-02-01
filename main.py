'''import qrook as qr



# same from config
qr.connect(adapter='postgres', host='localhost', port=3268, db='app-db', name='kurush', password='pondoxo')
qr.init()

# pagination, order by
# possible other import/reload
from qrook.data import *
books.select(id, name, publish_date).where(id < 10).all()
books.select('*').join(authors, id).join(authors, books.id==authors.id).one()
books.insert(x)  # dict or row-class
books.delete(x)  # dict or row-class'''

from qrlogging import logger
from connector.PostgresConnector import PostgresConnector

def main():
    '''logger.warning('AAA')
    logger.info('AAA')
    logger.error('AAA')'''

    db = PostgresConnector('qrdb', 'kurush', 'shizilla34')
    data = db.select('select * from books limit 5')
    print(data)
    print(db.table_info())


if __name__ == '__main__':
    main()