from rebel.exceptions import MixedPositionalAndNamedArguments


class SqlBuilderTestCase(object):

    def test_sql_builder_query(self):
        sql = self.db.sql('SELECT * FROM cities')
        sql.add('WHERE id > ?', 1)
        self.assertEqual(sql.query(), [
            {'id': 2, 'name': 'Washington'},
            {'id': 3, 'name': 'Los Angeles'},
        ])

    def test_sql_builder_query_one(self):
        sql = self.db.sql('SELECT * FROM cities')
        sql.add('WHERE id = ?', 2)
        self.assertEqual(sql.query_one(), {'id': 2, 'name': 'Washington'})

    def test_sql_builder_query_value(self):
        sql = self.db.sql('SELECT name FROM cities')
        sql.add('WHERE id = ?', 3)
        self.assertEqual(sql.query_value(), 'Los Angeles')

    def test_sql_builder_query_values(self):
        sql = self.db.sql('SELECT name FROM cities')
        sql.add('WHERE id > 0')
        cities = sql.query_values()
        self.assertEqual(sql.query_values(), ['New York', 'Washington', 'Los Angeles'])

    def test_sql_builder_execute(self):
        sql = self.db.sql('INSERT INTO users (email)')
        sql.add('values (?)', 'foo@bar.com').execute()
        user = self.db.query_one('SELECT * from users')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_sql_builder_back(self):
        emails = ['foo@bar.com', 'bar@foo.com']
        sql = self.db.sql('INSERT INTO users (email) VALUES')
        for email in emails:
            sql.add('(?)', email).add(',')
        sql.back()
        sql.execute()
        users = self.db.query('SELECT * FROM users')
        self.assertEqual(users, [
            {'id': 1, 'email': 'foo@bar.com'},
            {'id': 2, 'email': 'bar@foo.com'},
        ])

    def test_sql_builder_with_named_arguments(self):
        sql = self.db.sql('SELECT * FROM cities')
        sql.add('WHERE id = :id', id=2)
        self.assertEqual(sql.query_one(), {'id': 2, 'name': 'Washington'})

    def test_sql_builder_with_repeated_named_arguments_across_add_calls(self):
        users = [{'email': 'foo@bar.com'}, {'email': 'bar@foo.com'}]
        sql = self.db.sql('INSERT INTO users (email) VALUES')
        for user in users:
            sql.add('(:email)', email=user['email'])
            sql.add(',')
        sql.back()
        sql.execute()
        database_users = self.db.query('SELECT * FROM users ORDER BY id')
        self.assertEqual(database_users, [
            {'id': 1, 'email': 'foo@bar.com'},
            {'id': 2, 'email': 'bar@foo.com'},
        ])

    def test_sql_builder_cannot_mix_positional_and_named_arguments(self):
        sql = self.db.sql('SELECT * FROM cities')
        with self.assertRaises(MixedPositionalAndNamedArguments):
            sql.add('WHERE id = ?, name = :name', 1, name='New York')
