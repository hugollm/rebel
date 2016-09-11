class PgsqlDriver(object):

    def __init__(self, host=None, port=None, database=None, user=None, password=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self, host=None, database=None, user=None, password=None, port=None):
        import psycopg2
        self.connection = psycopg2.connect(
            host = self.host,
            port = self.port,
            database = self.database,
            user = self.user,
            password = self.password
        )
        self.connection.set_session(autocommit=True)

    def query(self, sql, args):
        sql = sql.replace('?', '%s')
        cursor = self.connection.cursor()
        cursor.execute(sql, args)
        return cursor

    def start_transaction(self, isolation_level):
        self.connection.set_session(isolation_level=isolation_level, autocommit=False)

    def commit(self):
        self.connection.commit()
        self.connection.set_session(isolation_level='DEFAULT', autocommit=True)

    def rollback(self):
        self.connection.rollback()
        self.connection.set_session(isolation_level='DEFAULT', autocommit=True)
