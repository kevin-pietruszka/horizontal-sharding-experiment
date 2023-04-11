class InvalidPredicate(Exception):
    "Raised when predicate was not properly assigned"
    pass

class InvalidInput(Exception):
    "Raised when query value is not allowed"
    pass

class NoConnections(Exception):
    "Raised no connections have been establish"
    pass