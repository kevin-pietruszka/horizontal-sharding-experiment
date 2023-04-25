import pandas as pd
from controller_worker.Worker import Worker, outputs
import threading
from typing import Union
from util.errors import InvalidInput
from sharding_methods.EvenSplit import even_split

class Controller:
    
    def __init__(self, data: pd.DataFrame, num_splits:int=5) -> None:
        
        self.num_splits = num_splits
        
        self.shards = []
        self.threads = []

        splits = even_split(data, num_splits)

        for index, df in enumerate(splits):
            self.shards.append(Worker(index, df))
            
    def execute_query(self, query:Union[str, tuple]) -> pd.DataFrame:
        
        if type(query) == str:
            
            if query == "done":
                for shard in self.shards:
                    shard.add_query(query)
                
                return "Process ended"
            
            raise InvalidInput
        
        for shard in self.shards:
            shard.add_query(query)
        
        output = self.build_output()
        return output

    def build_output(self) -> pd.DataFrame:
        
        df = pd.DataFrame()
        
        for i in range(self.num_splits):
            res = outputs.get()
            df = pd.concat([df, res])
            
        return df

    def start_threads(self):
        
        for shard in self.shards:
            x = threading.Thread(target=shard.listen)
            self.threads.append(x)
            x.start()