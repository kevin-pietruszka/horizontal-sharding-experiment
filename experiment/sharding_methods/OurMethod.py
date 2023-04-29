import pandas as pd
import numpy as np
from typing import List

def our_split(df: pd.DataFrame, stats: np.ndarray) -> List[pd.DataFrame]:
    # df is dataframe to be split
    # stats is ndarray of size (number of rows in df, num of shards)
    pass