import pandas as pd
import numpy as np
from typing import List

def our_split(df: pd.DataFrame, stats: np.ndarray) -> List[pd.DataFrame]:
    # df is dataframe to be split
    # stats is ndarray of size (number of rows in df, num of shards)

    
    row_map = {}
    num_rows, num_shards = stats.shape
    
    max_per_shard =  int((1 / float(num_shards)) * float(num_rows) * 1.2)


    #init row list for each shard
    for i in range(num_shards):
        row_map[i] = []

    #loop thru rows, sort shards by most frequent
    for i, row in enumerate(stats):
        
        sorted_args = np.flip(np.argsort(row))
        

        #find shard that most frequently uses row that is not full
        for arg in sorted_args:
            if (len(row_map[arg]) < max_per_shard):
                row_map[arg].append(i)
                break
    
    output = []
    for i in range(num_shards):
        output.append(df.iloc[row_map[i]])

        
    return output