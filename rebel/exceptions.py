class NotInsideTransaction(Exception):

    def __init__(self):
        message = 'Trying to perform an operation that needs to be inside transaction'
        super(NotInsideTransaction, self).__init__(message)


class MixedPositionalAndNamedArguments(Exception):

    def __init__(self):
        message = 'Cannot mix positional and named arguments in query'
        super(MixedPositionalAndNamedArguments, self).__init__(message)
