from util.errors import InvalidPredicate
import sympy

def query_in_interval(predicate: str, value: int, interval: sympy.Interval) -> bool:
    
    pred = None
    x = sympy.Symbol('x')
    
    if (predicate == "LT"):
        pred = (x < value)
    elif (predicate == "LE"):
        pred = (x <= value)
    elif (predicate == "EQ"):
        pred = (x == value)
    elif (predicate == "NE"):
        pred = (x != value)
    elif (predicate == "GT"):
        pred = (x > value)
    elif (predicate == "GE"):
        pred = (x >= value)
    else:
        raise InvalidPredicate
    
    pred = pred.as_set()
    output = interval.intersect(pred)
    return output.is_empty