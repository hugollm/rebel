from .query_tests import QueryTestCase
from .sql_builder_tests import SqlBuilderTestCase
from .transaction_tests import TransactionTestCase
from rebel.database import Database


class DatabaseTestCase(QueryTestCase, SqlBuilderTestCase, TransactionTestCase):

    def setUp(self):
        driver = self.get_driver()
        self.db = Database(driver)
        self.create_tables()
        self.clear_tables()
        self.fill_cities()

    def fill_cities(self):
        self.db.execute("""
            INSERT INTO cities (name)
            VALUES (?), (?), (?)
        """, 'New York', 'Washington', 'Los Angeles')
