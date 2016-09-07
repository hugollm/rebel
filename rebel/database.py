import re

from .sql_builder import SqlBuilder
from .transaction import Transaction
from .exceptions import NotInsideTransaction, MixedPositionalAndNamedArguments


class Database(object):

    READ_UNCOMMITTED = 'READ UNCOMMITTED'
    READ_COMMITTED = 'READ COMMITTED'
    REPEATABLE_READ = 'REPEATABLE READ'
    SERIALIZABLE = 'SERIALIZABLE'

    def __init__(self, driver):
        self.driver = driver
        self.connected = False
        self.transaction_depth = 0
        self.rollback_issued = False

    def sql(self, sql_string, *args, **kwargs):
        sql = SqlBuilder(self)
        sql.add(sql_string, *args, **kwargs)
        return sql

    def query(self, sql, *args, **kwargs):
        self._connect_once()
        sql, args = self._parse_kwargs(sql, args, kwargs)
        cursor = self.driver.query(sql, args)
        rows = self._fetch_rows_from_cursor(cursor)
        cursor.close()
        return rows

    def _parse_kwargs(self, sql, args, kwargs):
        if args and kwargs:
            raise MixedPositionalAndNamedArguments()
        if not kwargs:
            return sql, args
        key_pattern = ':[a-zA-Z_]+'
        keys = re.findall(key_pattern, sql)
        sql = re.sub(key_pattern, '?', sql)
        args = []
        for key in keys:
            arg = kwargs[key[1:]]
            args.append(arg)
        return sql, args

    def _fetch_rows_from_cursor(self, cursor):
        columns = [column[0] for column in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(dict(zip(columns, row)))
        return rows

    def execute(self, sql, *args, **kwargs):
        self._connect_once()
        sql, args = self._parse_kwargs(sql, args, kwargs)
        cursor = self.driver.query(sql, args)
        cursor.close()

    def query_one(self, sql, *args, **kwargs):
        rows = self.query(sql, *args, **kwargs)
        return rows[0] if rows else None

    def query_value(self, sql, *args, **kwargs):
        row = self.query_one(sql, *args, **kwargs)
        return next(iter(row.values())) if row else None

    def query_values(self, sql, *args, **kwargs):
        rows = self.query(sql, *args, **kwargs)
        values = []
        for row in rows:
            value = next(iter(row.values()))
            values.append(value)
        return values

    def transaction(self, isolation_level=None):
        return Transaction(self, isolation_level)

    def start_transaction(self, isolation_level=None):
        self._connect_once()
        if not self._inside_transaction():
            self.driver.start_transaction(isolation_level)
            self.rollback_issued = False
        self.transaction_depth += 1

    def commit(self):
        if not self._inside_transaction():
            raise NotInsideTransaction()
        if self.transaction_depth == 1 and not self.rollback_issued:
            self.driver.commit()
        if self.transaction_depth == 1 and self.rollback_issued:
            self.driver.rollback()
        self.transaction_depth -= 1

    def rollback(self):
        if not self._inside_transaction():
            raise NotInsideTransaction()
        if self.transaction_depth == 1:
            self.driver.rollback()
        self.transaction_depth -= 1
        self.rollback_issued = True

    def _connect_once(self):
        if self.connected:
            return
        self.driver.connect()
        self.connected = True

    def _inside_transaction(self):
        return self.transaction_depth > 0
