class SqlBuilder(object):

    def __init__(self, database):
        self.database = database
        self.parts = []

    def add(self, sql, *args, **kwargs):
        sql, args = self.database._parse_kwargs(sql, args, kwargs)
        self.parts.append({
            'sql': sql,
            'args': args,
        })
        return self

    def back(self):
        self.parts.pop()

    def query(self):
        sql, args = self._join_parts()
        return self.database.query(sql, *args)

    def query_one(self):
        sql, args = self._join_parts()
        return self.database.query_one(sql, *args)

    def query_value(self):
        sql, args = self._join_parts()
        return self.database.query_value(sql, *args)

    def query_values(self):
        sql, args = self._join_parts()
        return self.database.query_values(sql, *args)

    def execute(self):
        sql, args = self._join_parts()
        self.database.execute(sql, *args)

    def _join_parts(self):
        sql = ''
        args = []
        for part in self.parts:
            sql += part['sql'] + ' '
            args += part['args']
        return sql.strip(), args
