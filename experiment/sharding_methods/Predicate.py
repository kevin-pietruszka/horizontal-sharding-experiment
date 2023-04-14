
import pandas as pd
import sympy as sp
from typing import List

def predicate_split(df: pd.DataFrame, column: str, predicates: List[sp.Interval]):
    
    outputs = []
    num_splits = len(predicates)
    
    
    
    for i in range(num_splits):
        interval = predicates[i]
        print(interval)
        indices = []
        
        for index, val in enumerate(df[column]):
            
            if interval.contains(val):
                indices.append(index)
                
        
        outputs.append(df.iloc[indices])
        
    return outputs