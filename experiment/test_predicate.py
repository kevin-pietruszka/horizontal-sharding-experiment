
import numpy as np
import pandas as pd
import sympy
from sharding_methods.Predicate import predicate_split

def main():

    ids = np.array(range(1,11))
    ratings = np.random.rand(10)
    ratings = np.multiply(ratings, 10)

    df = pd.DataFrame()
    df["id"] = ids
    df["rating"] = ratings

    x = sympy.Symbol('x')
    int1 = (0 <= x) & (x <= 2.5)
    int2 = (2.5 <x) & (x <= 5)
    int3 = (5 < x) & (x <= 7.5)
    int4 = (7.5 < x) & (x <= 10)

    preds = [int1.as_set(), int2.as_set(), int3.as_set(), int4.as_set()]

    frames = predicate_split(df,'rating', preds)
    
    for df in frames:
        print(df)
    
if __name__ == '__main__':
    main()