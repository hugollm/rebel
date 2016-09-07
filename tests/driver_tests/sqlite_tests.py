from unittest import TestCase
from ..database_tests import DatabaseTestCase
from rebel.drivers.sqlite import SqliteDriver


class SqliteTestCase(DatabaseTestCase, TestCase):

    def get_driver(self):
        return SqliteDriver(database=':memory:')

    def create_tables(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS test_cities (
                id INTEGER PRIMARY KEY,
                name VARCHAR(254)
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS test_users (
                id INTEGER PRIMARY KEY,
                email VARCHAR(254)
            )
        """)

    def clear_tables(self):
        self.db.execute('DELETE FROM test_cities')
        self.db.execute('DELETE FROM test_users')

    def test_select_last_insert_id(self):
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        id = self.db.query_value('SELECT last_insert_rowid()')
        self.assertEqual(id, 1)
