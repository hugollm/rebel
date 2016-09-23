# Rebel

Rebel against the ORM empire with vanilla SQL for Python.

[![Build Status](https://travis-ci.org/hugollm/rebel.svg?branch=master)](https://travis-ci.org/hugollm/rebel)
[![Coverage Status](https://coveralls.io/repos/github/hugollm/rebel/badge.svg?branch=master)](https://coveralls.io/github/hugollm/rebel?branch=master)


## Overview

Rebel makes it simple to connect to a database and use vanilla SQL in Python. It's no rocket science, just a very thin layer of abstraction to make things fun again. Here's a peek on how to use it:

```python
from rebel import Database, PgsqlDriver

driver = PgsqlDriver(database='rebel', user='postgres')
db = Database(driver)

db.execute('insert into users (name) values (?), (?)', 'John', 'Jane')
db.query('select * from users') # [{id: 1, name: John}, {id: 2, name: Jane}]
db.query_one('select * from users where id = ?', 1) # {id: 1, name: John}
db.query_value('select id from users where id = :id', id=1) # 1
db.query_values('select id from users') # [1, 2]
```

The connection only happens right before the first query. So you are free to prepare the `database` object without the cost.


## Install

Rebel is available in the Python Package Index:

    pip install rebel


## Connect to Sqlite

To connect to an sqlite database, you're gonna be using the `SqliteDriver`. It does not have any external dependencies.

```python
from rebel import Database, SqliteDriver

driver = SqliteDriver('mydb.sqlite')
db = Database(driver)
```

The previous example will create a `mydb.sqlite` file if it doesn't exist already. If you wish to connect in memory:

```python
driver = SqliteDriver(':memory:')
```


## Connect to Postgresql

To connect to Postgresql, you'll be using the `PgsqlDriver`. First, you will need to install the package `psycopg2`:

    pip install psycopg2

Then, instantiate a driver and create the database object:

```python
from rebel import Database, PgsqlDriver

driver = PgsqlDriver(host='myhost.com', database='mydb', user='myuser', password='mypass')
db = Database(driver)
```

If your database uses a different port rather than the default, you can specify it as well:

```python
driver = PgsqlDriver(host='myhost.com', port=5569, database='mydb', user='myuser', password='mypass')
```

You can also connect via Unix-domain sockets (common in development). Just ommit the host and the password:

```python
driver = PgsqlDriver(database='mydb', user='postgres')
```


## Queries

The `execute` method will execute an sql statement and return nothing.

```python
db.execute('insert into users (name) values (?), (?)', 'John', 'Jane') # None
```

The `query` method will return a list of dictionaries (a table), or an empty list.

```python
db.query('select * from users') # [{id: 1, name: John}, {id: 2, name: Jane}]
db.query('select * from cities') # []
```

The `query_one` method will return one dictionary (a row) or `None`.

```python
db.query_one('select * from users where id = :id', id=1) # {id: 1, name: John}
db.query_one('select * from cities limit 1') # None
```

The `query_value` method will return a single value, or `None`.

```python
db.query_value('select 1') # 1
db.query_value('select name from users limit 1') # 'John'
db.query_value('select name from cities limit 1') # None
```

The `query_values` method will return a list of values (a column) or an empty list.

```python
db.query_values('select id from users') # [1, 2]
db.query_values('select id from cities') # []
```


## Parameters

Using vanilla SQL, you should never concatenate your parameters in the query. This would open you to SQL injection vulnerabilities.

Rebel offers you two ways to pass arguments to queries. Positional and named arguments. You **may not** use both in the same query (it would raise an exception).

```python
db.query_one('select * from users where id = ?', 1) # {id: 1, name: 'John'}
db.query_one('select * from users where id = :id', id=1) # {id: 1, name: 'John'}
```

While positional arguments are fine for small queries, named arguments are more suited for the big ones:

```python
db.query("""
    select u.id, u.email from users as u
    inner join payments as p on p.user_id = u.id
    where u.name like :name_pattern
    and p.value > :min_value
    order by u.id
    limit 50
""", name_pattern='J%', min_value=300)
```


## SQL Builder

Sometimes, we need to build complex queries while looping some list of data. For those needs, you can use the sql builder to fill the querie piece by piece:

```python
names = ['John', 'Jane', 'Foo', 'Bar']
sql = db.sql('insert into users (name) values')
for name in names:
    sql.add('(?)', name).add(',')
sql.back() # remve the extra comma
sql.execute()
```

The `SqlBuilder` object has the same query methods as the database, with the same return values. As you can see from the example, you'll be using the method `add` to build the query, and the `back` method to remove the last added piece (including arguments).


## Transactions

Rebel ships with a nice syntax for transactions:

```python
with db.transaction():
    db.execute('insert into users (name) values (?)', 'John')
    db.execute('insert into users (name) values (?)', 'Jane')
```

If an exception of any kind is raised inside the transaction, it will automatically rollback. You can also control the flux manually if you like:

```python
db.start_transaction()
try:
    db.execute('insert into users (name) values (?)', 'John')
    db.execute('insert into users (name) values (?)', 'Jane')
    db.commit()
except:
    db.rollback()
    raise
```

You are free to nest transactions with both syntaxes. Very useful in tests:

```python
class MyTestCase(TestCase):

    def setUp(self):
        db.start_transaction()

    def tearDown(self):
        db.rollback()

    def test_my_feature(self):
        result = function_with_transaction_inside()
        self.assertTrue(result)
```

In the example above, every test is free to issue queries without worrying about cleanup, because the test will always run inside a transaction (that will rollback). The interesting thing to note here: the tested code is free to issue transactions of it's own, without ever knowing they will run inside another. If any transaction rollbacks inside the "nest", the whole thing will rollback.

If you wish to specify the isolation level for a transaction:

```python
with db.transaction(db.SERIALIZABLE):
    db.execute('insert into users (name) values (?)', 'John')
    db.execute('insert into users (name) values (?)', 'Jane')
```

The argument works with `start_transaction` method as well. The possible values are `READ_UNCOMMITTED`, `READ_COMMITTED`, `REPEATABLE_READ` and `SERIALIZABLE`. This argument will be ignored by `SqliteDriver`.
