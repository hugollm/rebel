from rebel.database import Database
from rebel.exceptions import NotInsideTransaction, MixedPositionalAndNamedArguments


class DatabaseTestCase(object):

    def setUp(self):
        driver = self.get_driver()
        self.db = Database(driver)
        self.create_tables()
        self.clear_tables()
        self.fill_cities()

    def get_settings(self):
        raise NotImplementedError()

    def clear_tables(self):
        raise NotImplementedError()

    def create_tables(self):
        raise NotImplementedError()

    def fill_cities(self):
        self.db.execute("""
            INSERT INTO test_cities (name)
            VALUES (?), (?), (?)
        """, 'New York', 'Washington', 'Los Angeles')

    def test_query_empty_table_returns_empty_list(self):
        list = self.db.query('SELECT * FROM test_users')
        self.assertEqual(list, [])

    def test_query_filled_table_returns_list(self):
        list = self.db.query('SELECT * FROM test_cities')
        self.assertEqual(len(list), 3)

    def test_query_one_returns_dict(self):
        city = self.db.query_one('SELECT * FROM test_cities WHERE id = ?', 1)
        self.assertEqual(city, {'id': 1, 'name': 'New York'})

    def test_query_value(self):
        name = self.db.query_value('SELECT name FROM test_cities WHERE id = ?', 1)
        self.assertEqual(name, 'New York')

    def test_query_values(self):
        names = self.db.query_values('SELECT name FROM test_cities ORDER BY id')
        self.assertEqual(names, ['New York', 'Washington', 'Los Angeles'])

    def test_execute_statement_with_arguments(self):
        self.db.execute("""
            INSERT INTO test_users (id, email)
            VALUES (?, ?)
        """, 1, 'foo@bar.com')
        user = self.db.query_one('SELECT * FROM test_users LIMIT 1')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_execute_statement_with_named_arguments(self):
        self.db.execute("""
            INSERT INTO test_users (id, email)
            VALUES (:id, :email)
        """, id=1, email='foo@bar.com')
        user = self.db.query_one('SELECT * FROM test_users LIMIT 1')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_execute_statement_with_repeated_named_arguments(self):
        self.db.execute("""
            INSERT INTO test_cities (name)
            VALUES (:name), (:name)
        """, name='California')
        cities = self.db.query("""
            SELECT * FROM test_cities
            WHERE name = :name ORDER BY id
        """, name='California')
        self.assertEqual(cities, [
            {'id': 4, 'name': 'California'},
            {'id': 5, 'name': 'California'}
        ])

    def test_query_statement_with_named_arguments(self):
        cities = self.db.query('SELECT * FROM test_cities WHERE name = :name', name='Los Angeles')
        self.assertEqual(cities, [{'id': 3, 'name': 'Los Angeles'}])

    def test_query_one_with_named_arguments(self):
        city = self.db.query_one('SELECT * FROM test_cities WHERE name = :name', name='Los Angeles')
        self.assertEqual(city, {'id': 3, 'name': 'Los Angeles'})

    def test_query_value_with_named_arguments(self):
        name = self.db.query_value('SELECT name FROM test_cities WHERE id = :id', id=3)
        self.assertEqual(name, 'Los Angeles')

    def test_query_values_with_named_arguments(self):
        names = self.db.query_values('SELECT name FROM test_cities WHERE id > :id ORDER BY id', id=0)
        self.assertEqual(names, ['New York', 'Washington', 'Los Angeles'])

    def test_cannot_mix_positional_and_named_arguments_in_execute_statement(self):
        with self.assertRaises(MixedPositionalAndNamedArguments):
            self.db.execute("""
                INSERT INTO test_users (id, email)
                VALUES (?, :email)
            """, 1, email='foo@bar.com')

    def test_cannot_mix_positional_and_named_arguments_in_query_statement(self):
        with self.assertRaises(MixedPositionalAndNamedArguments):
            self.db.query("""
                SELECT * FROM test_cities
                WHERE id = ? and name = :name
            """, 1, name='New York')

    def test_insert_and_query(self):
        users = [
            {'id': 1, 'email': 'email1@email.com'},
            {'id': 2, 'email': 'email2@email.com'},
        ]
        for user in users:
            self.db.execute(
                'insert into test_users (id, email) values (?, ?)',
                user['id'], user['email']
            )
        result_users = self.db.query('SELECT * FROM test_users ORDER BY id')
        self.assertEqual(result_users, users)

    def test_sql_builder_query(self):
        sql = self.db.sql('SELECT * FROM test_cities')
        sql.add('WHERE id > ?', 1)
        self.assertEqual(sql.query(), [
            {'id': 2, 'name': 'Washington'},
            {'id': 3, 'name': 'Los Angeles'},
        ])

    def test_sql_builder_query_one(self):
        sql = self.db.sql('SELECT * FROM test_cities')
        sql.add('WHERE id = ?', 2)
        self.assertEqual(sql.query_one(), {'id': 2, 'name': 'Washington'})

    def test_sql_builder_query_value(self):
        sql = self.db.sql('SELECT name FROM test_cities')
        sql.add('WHERE id = ?', 3)
        self.assertEqual(sql.query_value(), 'Los Angeles')

    def test_sql_builder_query_values(self):
        sql = self.db.sql('SELECT name FROM test_cities')
        sql.add('WHERE id > 0')
        cities = sql.query_values()
        self.assertEqual(sql.query_values(), ['New York', 'Washington', 'Los Angeles'])

    def test_sql_builder_execute(self):
        sql = self.db.sql('INSERT INTO test_users (email)')
        sql.add('values (?)', 'foo@bar.com').execute()
        user = self.db.query_one('SELECT * from test_users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_sql_builder_back(self):
        emails = ['foo@bar.com', 'bar@foo.com']
        sql = self.db.sql('INSERT INTO test_users (email) VALUES')
        for email in emails:
            sql.add('(?)', email).add(',')
        sql.back()
        sql.execute()
        users = self.db.query('SELECT * FROM test_users')
        self.assertEqual(users, [
            {'id': 1, 'email': 'foo@bar.com'},
            {'id': 2, 'email': 'bar@foo.com'},
        ])

    def test_sql_builder_with_named_arguments(self):
        sql = self.db.sql('SELECT * FROM test_cities')
        sql.add('WHERE id = :id', id=2)
        self.assertEqual(sql.query_one(), {'id': 2, 'name': 'Washington'})

    def test_sql_builder_with_repeated_named_arguments_across_add_calls(self):
        users = [{'email': 'foo@bar.com'}, {'email': 'bar@foo.com'}]
        sql = self.db.sql('INSERT INTO test_users (email) VALUES')
        for user in users:
            sql.add('(:email)', email=user['email'])
            sql.add(',')
        sql.back()
        sql.execute()
        database_users = self.db.query('SELECT * FROM test_users ORDER BY id')
        self.assertEqual(database_users, [
            {'id': 1, 'email': 'foo@bar.com'},
            {'id': 2, 'email': 'bar@foo.com'},
        ])

    def test_sql_builder_cannot_mix_positional_and_named_arguments(self):
        sql = self.db.sql('SELECT * FROM test_cities')
        with self.assertRaises(MixedPositionalAndNamedArguments):
            sql.add('WHERE id = ?, name = :name', 1, name='New York')

    def test_commit(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_rollback(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertIsNone(user)

    def test_commit_outside_transaction_raises_exception(self):
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        with self.assertRaises(NotInsideTransaction):
            self.db.commit()

    def test_rollback_outside_transaction_raises_exception(self):
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        with self.assertRaises(NotInsideTransaction):
            self.db.rollback()

    def test_two_commits_for_one_transaction_level_raises_exception(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        with self.assertRaises(NotInsideTransaction):
            self.db.commit()

    def test_two_rollbacks_for_one_transaction_level_raises_exception(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        with self.assertRaises(NotInsideTransaction):
            self.db.rollback()

    def test_commits_in_nested_transaction(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        self.db.commit()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_rollbacks_in_nested_transaction(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertIsNone(user)

    def test_commit_then_rollback_in_nested_transaction_results_in_rollback(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertIsNone(user)

    def test_rollback_then_commit_in_nested_transaction_results_in_rollback(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        self.db.commit()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertIsNone(user)

    def test_data_is_available_before_rollback_and_not_after_in_nested_transactions(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertIsNone(user)

    def test_database_is_in_autocommit_mode_outside_transaction(self):
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.driver.rollback()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_database_returns_to_autocommit_mode_after_transaction(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'bar@foo.com')
        self.db.driver.rollback()
        email = self.db.query_value('SELECT email FROM test_users')
        self.assertEqual(email, 'bar@foo.com')

    def test_database_transaction_with_informed_isolation_level(self):
        self.db.start_transaction(self.db.REPEATABLE_READ)
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertIsNone(user)

    def test_all_isolation_levels(self):
        self.db.start_transaction(self.db.READ_UNCOMMITTED)
        self.db.start_transaction(self.db.READ_COMMITTED)
        self.db.start_transaction(self.db.REPEATABLE_READ)
        self.db.start_transaction(self.db.SERIALIZABLE)
        self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        self.db.rollback()
        self.db.rollback()
        self.db.rollback()
        with self.assertRaises(NotInsideTransaction):
            self.db.rollback()

    def test_managed_transaction_automatically_commits(self):
        with self.db.transaction():
            self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
        self.db.driver.rollback()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_managed_transaction_rollback_if_exception_propagates(self):
        test_exception_class = type('TestException', (Exception,), {})
        with self.assertRaises(test_exception_class):
            with self.db.transaction():
                self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
                raise test_exception_class()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertIsNone(user)

    def test_managed_transaction_rollback_if_exception_propagates_from_nested_transaction(self):
        test_exception_class = type('TestException', (Exception,), {})
        with self.assertRaises(test_exception_class):
            with self.db.transaction():
                self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
                with self.db.transaction():
                    self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
                    raise test_exception_class()
        user = self.db.query_one('SELECT * FROM test_users')
        self.assertIsNone(user)

    def test_database_leaves_transaction_mode_if_exception_propagates_in_managed_transaction(self):
        test_exception_class = type('TestException', (Exception,), {})
        with self.assertRaises(test_exception_class):
            with self.db.transaction():
                self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
                with self.db.transaction():
                    self.db.execute('INSERT INTO test_users (email) VALUES (?)', 'foo@bar.com')
                    raise test_exception_class()
        self.assertEqual(self.db.transaction_depth, 0)
