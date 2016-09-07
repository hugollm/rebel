import sqlite3


class SqliteDriver(object):

    def __init__(self, database):
        self.database = database

    def connect(self):
        self.connection = sqlite3.connect(self.database)
        self.autocommit = True

    def query(self, sql, args):
        cursor = self.connection.cursor()
        cursor.execute(sql, args)
        if self.autocommit:
            self.commit()
        return cursor

    def start_transaction(self, isolation_level):
        self.autocommit = False

    def commit(self):
        self.connection.commit()
        self.autocommit = True

    def rollback(self):
        self.connection.rollback()
        self.autocommit = True
