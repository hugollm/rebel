class Transaction(object):

    def __init__(self, database, isolation_level):
        self.database = database
        self.isolation_level = isolation_level

    def __enter__(self):
        self.database.start_transaction(self.isolation_level)

    def __exit__(self, exception_type, exception, traceback):
        if exception:
            self.database.rollback()
        else:
            self.database.commit()
