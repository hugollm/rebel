from unittest import TestCase
from ..database_tests import DatabaseTestCase
from rebel.drivers.pgsql import PgsqlDriver


class PgsqlTestCase(DatabaseTestCase, TestCase):

    def get_driver(self):
        return PgsqlDriver(database='rebel', user='postgres')

    def create_tables(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS test_cities (
                id SERIAL PRIMARY KEY,
                name VARCHAR(254)
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS test_users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(254)
            )
        """)

    def clear_tables(self):
        self.db.execute('TRUNCATE TABLE test_cities RESTART IDENTITY')
        self.db.execute('TRUNCATE TABLE test_users RESTART IDENTITY')

    def test_select_last_insert_id(self):
        id = self.db.query_value('INSERT INTO test_users (email) VALUES (?) RETURNING id', 'foo@bar.com')
        self.assertEqual(id, 1)
