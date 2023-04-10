import pandas as pd
from collections.abc import Sequence

def even_split(data: pd.DataFrame, num_splits:int) -> Sequence[pd.DataFrame]:

    output = []
    split_size = int(len(data) / num_splits)

    for i in range(0, len(data), split_size):
            
        df = data.iloc[i: i + split_size]

        output.append(df)
    
    return output