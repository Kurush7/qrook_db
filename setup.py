import setuptools

setuptools.setup(
    name="qrookDB",
    version="1.3.3.1",
    author="Kurush",
    author_email="ze17@ya.ru",
    description="tiny ORM for SQL-databases",
    long_description_content_type="text/markdown",
    url="https://github.com/Kurush7/qrook_db",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    long_description='''
    
# Initializing
This package represents a new ORM to work with SQL-syntax databases (for now, only PostgreSQL and SQLite are natively supported,
but you may inherit your own connectors from IConnector abstract class).

To start working, all you need to do is import a package's facade object and initialise it:
```python
import qrookDB.DB as DB
DB = DB.DB('postgres', 'db_name', 'username', 'password', format_type='dict')
DB.create_logger(app_name='qrookdb_test', file='app_log.log')
DB.create_data(__name__, in_module=True)
```
Here we first created a database connection, providing connect parameters (format_type defines the form
in which results'll be returned - 'list' and 'dict' supported),
then initialized an internal logger to write both in console and file.
Note that instead of 'postgres' you could've sent the instance of the connector (including your own connectors).
The 'create_data' function reads database system tables to get info about all user-defined tables,
and creates QRTable objects based on this info. Now you can use table names (ones given to them in database)
to access to these objects as DB instance fields (also, configuration showed above adds these table names to
your current module, so you can use short names: 'books' instead of 'DB.books').   

# Querying
You can form execute queries using either DB instance or concrete tables (in this case, you don't need to
mention in which table to perform queries). To execute any query, use one of 'exec', 'one' and 'all' methods
(latter two define how many rows to return from query; 'exec' returns None by default). If forming of the query
fails, it won't be executed at all (and will return None if you try); you can use 'get_error' method of the query
to get the error description (it will be logged, though). Note: if error occured in the middle of query-building,
query will ignore the rest of building proccess.
## Select queries examples
```python
op = DB.operators

print(DB.books)
print(books, books.id)

# logical 'and' is used by default for multiple where conditions; 'op' module contains special operators
data = DB.select(books).where(original_publication_year=2000, language_code='eng').\
    where(id=op.In(470, 490, 485)).all()

# you can add raw-string query parts, but it'll be on your conscience in terms of security   
query = books.select('count(*)').group_by(books.original_publication_year)
data = query.all()

data = DB.select(books, books.id).where('id < 10').order_by(books.id, desc=True).\
    limit(3).offset(2).all()

# here fields have same name ('id'), but via different tables it'll be ok
# (for data in dict-format, table-names'll be added to keys) 
data = books.select(authors.id, books.id)\
    .join(books_authors, op.Eq(books_authors.book_id, books.id))\
    .join(authors, op.Eq(books_authors.author_id, authors.id)).all()
    # .join(books_authors, 'books_authors.book_id = books.id')\

data = books.select(books.id).where(id=1).where(bool='or', id=2).all()

# error - trying to select two equal fields;
q = DB.select(events, events.id, events.id).where(id=1)
data = q.all()
print('data is None here:', data, ';\terror:', q.get_error())
```


## Update, Insert, Delete queries examples
```python
# if auto_commit is not set, you'll have to commit manually 
ok = DB.delete(events, auto_commit=True).where(id=1).exec()

from datetime import datetime
t = datetime.now().time()
d = datetime.now().date()
ok = DB.update(events, auto_commit=False).set(time=t).where(id=6).exec()
DB.commit()

# other possible variants for values: values([t]), values([d, t])
# other possible variants for returning: returning(events.date, events.time), returning(['date', 'time']), returning('date', 'time')
query = events.insert(events.date, events.time, auto_commit=True).values([[d, t], [None, t]]).returning('*')
data = query.all()

# you can also execute fully-raw queries; if you need to return values, 
use 'config_fields' to define results' names (not necessary for 'list' data format) 
data = DB.exec('select * from get_book_authors(1) as f(id int, name varchar)').config_fields('id', 'name').all()
print(data)
```

## Table's meta-data
Using this package you can easily discover your database. DB instance provides information
about table's name, its columns (with their name and type) and info about primary and
foreign keys. Example of function which prints all info about existing tables is shown below.
```python
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
```

and the result can be:
```shell
===--- publications ---===
    book_id: integer
    isbn: bigint
(*) id: integer
    created_at: timestamp with time zone
    isbn13: bigint
    language_code: character varying
    publication_year: smallint
    info: jsonb
    updated_at: timestamp with time zone
Constraints:
	Foreign key: book_id references books(id)
```

'''
)
