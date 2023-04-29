from util.predicates import query_in_interval
import sympy

interval = sympy.Interval(0, 10)


print(query_in_interval('GT', 0, interval))