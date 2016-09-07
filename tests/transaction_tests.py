from rebel.exceptions import NotInsideTransaction


class TransactionTestCase(object):

    def test_commit(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        user = self.db.query_one('SELECT * FROM users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_rollback(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM users')
        self.assertIsNone(user)

    def test_commit_outside_transaction_raises_exception(self):
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        with self.assertRaises(NotInsideTransaction):
            self.db.commit()

    def test_rollback_outside_transaction_raises_exception(self):
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        with self.assertRaises(NotInsideTransaction):
            self.db.rollback()

    def test_two_commits_for_one_transaction_level_raises_exception(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        with self.assertRaises(NotInsideTransaction):
            self.db.commit()

    def test_two_rollbacks_for_one_transaction_level_raises_exception(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        with self.assertRaises(NotInsideTransaction):
            self.db.rollback()

    def test_commits_in_nested_transaction(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        self.db.commit()
        user = self.db.query_one('SELECT * FROM users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_rollbacks_in_nested_transaction(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM users')
        self.assertIsNone(user)

    def test_commit_then_rollback_in_nested_transaction_results_in_rollback(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM users')
        self.assertIsNone(user)

    def test_rollback_then_commit_in_nested_transaction_results_in_rollback(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        self.db.commit()
        user = self.db.query_one('SELECT * FROM users')
        self.assertIsNone(user)

    def test_data_is_available_before_rollback_and_not_after_in_nested_transactions(self):
        self.db.start_transaction()
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.commit()
        user = self.db.query_one('SELECT * FROM users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM users')
        self.assertIsNone(user)

    def test_database_is_in_autocommit_mode_outside_transaction(self):
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.driver.rollback()
        user = self.db.query_one('SELECT * FROM users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_database_returns_to_autocommit_mode_after_transaction(self):
        self.db.start_transaction()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'bar@foo.com')
        self.db.driver.rollback()
        email = self.db.query_value('SELECT email FROM users')
        self.assertEqual(email, 'bar@foo.com')

    def test_database_transaction_with_informed_isolation_level(self):
        self.db.start_transaction(self.db.REPEATABLE_READ)
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        user = self.db.query_one('SELECT * FROM users')
        self.assertIsNone(user)

    def test_all_isolation_levels(self):
        self.db.start_transaction(self.db.READ_UNCOMMITTED)
        self.db.start_transaction(self.db.READ_COMMITTED)
        self.db.start_transaction(self.db.REPEATABLE_READ)
        self.db.start_transaction(self.db.SERIALIZABLE)
        self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.rollback()
        self.db.rollback()
        self.db.rollback()
        self.db.rollback()
        with self.assertRaises(NotInsideTransaction):
            self.db.rollback()

    def test_managed_transaction_automatically_commits(self):
        with self.db.transaction():
            self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
        self.db.driver.rollback()
        user = self.db.query_one('SELECT * FROM users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_managed_transaction_rollback_if_exception_propagates(self):
        test_exception_class = type('TestException', (Exception,), {})
        with self.assertRaises(test_exception_class):
            with self.db.transaction():
                self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
                raise test_exception_class()
        user = self.db.query_one('SELECT * FROM users')
        self.assertIsNone(user)

    def test_managed_transaction_rollback_if_exception_propagates_from_nested_transaction(self):
        test_exception_class = type('TestException', (Exception,), {})
        with self.assertRaises(test_exception_class):
            with self.db.transaction():
                self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
                with self.db.transaction():
                    self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
                    raise test_exception_class()
        user = self.db.query_one('SELECT * FROM users')
        self.assertIsNone(user)

    def test_database_leaves_transaction_mode_if_exception_propagates_in_managed_transaction(self):
        test_exception_class = type('TestException', (Exception,), {})
        with self.assertRaises(test_exception_class):
            with self.db.transaction():
                self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
                with self.db.transaction():
                    self.db.execute('INSERT INTO users (email) VALUES (?)', 'foo@bar.com')
                    raise test_exception_class()
        self.assertEqual(self.db.transaction_depth, 0)
