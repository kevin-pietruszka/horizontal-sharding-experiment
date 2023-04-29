import pandas as pd
from typing import List
import numpy as np

class StatisticsTable:
    
    def __init__(self) -> None:
        self.counts = dict()
        
    def update(self, pd: pd.DataFrame) -> None:
        
        for i, row in pd.iterrows():
            
            id = row['id']
            
            self.counts[id] = self.counts.get(id, 0) + 1
    
    def get_counts(self):
        return self.counts
    
    @staticmethod
    def combine(num_tuples: int, shard_stats: List['StatisticsTable']) -> np.ndarray:
        
        num_shards = len(shard_stats)
        frequencies = np.zeros((num_tuples, num_shards))
        
        for i in range(num_shards):
            
            stats_dict = shard_stats[i].get_counts()
            
            for key, val in stats_dict.items():
                
                r = int(key) - 1
                frequencies[r, i] = val
        
        for j in range(num_shards):
            column = frequencies[:, j]
            _sum = np.sum(column)
            column = np.divide(column, _sum)
            frequencies[:, j] = column
        return frequencies