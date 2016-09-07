from rebel.exceptions import MixedPositionalAndNamedArguments


class QueryTestCase(object):

    def test_query_empty_table_returns_empty_list(self):
        list = self.db.query('SELECT * FROM users')
        self.assertEqual(list, [])

    def test_query_filled_table_returns_list(self):
        list = self.db.query('SELECT * FROM cities')
        self.assertEqual(len(list), 3)

    def test_query_one_returns_dict(self):
        city = self.db.query_one('SELECT * FROM cities WHERE id = ?', 1)
        self.assertEqual(city, {'id': 1, 'name': 'New York'})

    def test_query_value(self):
        name = self.db.query_value('SELECT name FROM cities WHERE id = ?', 1)
        self.assertEqual(name, 'New York')

    def test_query_values(self):
        names = self.db.query_values('SELECT name FROM cities ORDER BY id')
        self.assertEqual(names, ['New York', 'Washington', 'Los Angeles'])

    def test_execute_statement_with_arguments(self):
        self.db.execute("""
            INSERT INTO users (id, email)
            VALUES (?, ?)
        """, 1, 'foo@bar.com')
        user = self.db.query_one('SELECT * FROM users LIMIT 1')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_execute_statement_with_named_arguments(self):
        self.db.execute("""
            INSERT INTO users (id, email)
            VALUES (:id, :email)
        """, id=1, email='foo@bar.com')
        user = self.db.query_one('SELECT * FROM users LIMIT 1')
        self.assertEqual(user, {'id': 1, 'email': 'foo@bar.com'})

    def test_execute_statement_with_repeated_named_arguments(self):
        self.db.execute("""
            INSERT INTO cities (name)
            VALUES (:name), (:name)
        """, name='California')
        cities = self.db.query("""
            SELECT * FROM cities
            WHERE name = :name ORDER BY id
        """, name='California')
        self.assertEqual(cities, [
            {'id': 4, 'name': 'California'},
            {'id': 5, 'name': 'California'}
        ])

    def test_query_statement_with_named_arguments(self):
        cities = self.db.query('SELECT * FROM cities WHERE name = :name', name='Los Angeles')
        self.assertEqual(cities, [{'id': 3, 'name': 'Los Angeles'}])

    def test_query_one_with_named_arguments(self):
        city = self.db.query_one('SELECT * FROM cities WHERE name = :name', name='Los Angeles')
        self.assertEqual(city, {'id': 3, 'name': 'Los Angeles'})

    def test_query_value_with_named_arguments(self):
        name = self.db.query_value('SELECT name FROM cities WHERE id = :id', id=3)
        self.assertEqual(name, 'Los Angeles')

    def test_query_values_with_named_arguments(self):
        names = self.db.query_values('SELECT name FROM cities WHERE id > :id ORDER BY id', id=0)
        self.assertEqual(names, ['New York', 'Washington', 'Los Angeles'])

    def test_cannot_mix_positional_and_named_arguments_in_execute_statement(self):
        with self.assertRaises(MixedPositionalAndNamedArguments):
            self.db.execute("""
                INSERT INTO users (id, email)
                VALUES (?, :email)
            """, 1, email='foo@bar.com')

    def test_cannot_mix_positional_and_named_arguments_in_query_statement(self):
        with self.assertRaises(MixedPositionalAndNamedArguments):
            self.db.query("""
                SELECT * FROM cities
                WHERE id = ? and name = :name
            """, 1, name='New York')

    def test_insert_and_query(self):
        users = [
            {'id': 1, 'email': 'email1@email.com'},
            {'id': 2, 'email': 'email2@email.com'},
        ]
        for user in users:
            self.db.execute(
                'insert into users (id, email) values (?, ?)',
                user['id'], user['email']
            )
        result_users = self.db.query('SELECT * FROM users ORDER BY id')
        self.assertEqual(result_users, users)
