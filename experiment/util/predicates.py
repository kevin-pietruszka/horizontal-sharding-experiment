from util.errors import InvalidPredicate
import sympy
from typing import List

def predicate_interval(col_range: List[int], num_shards: int) -> List[sympy.Interval]:
    _min = col_range[0]
    _max = col_range[1]

    step: float = float(_max - _min) / num_shards

    output = list()
    curr_val = _min
    for i in range(num_shards):
        if i == 0:
            interval = sympy.Interval(curr_val, curr_val + step)
        else:
            interval = sympy.Interval(curr_val, curr_val + step, left_open=True)

        output.append(interval)
        curr_val += step

    return output

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