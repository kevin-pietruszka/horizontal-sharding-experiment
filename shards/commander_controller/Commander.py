import pandas as pd
from commander_controller.Controller import Controller, outputs
import threading
from typing import Union
from util.errors import InvalidInput

class Commander:
    
    def __init__(self, data: pd.DataFrame, num_splits:int=5) -> None:
        
        self.num_splits = num_splits
        split_size = int(len(data) / self.num_splits)
        
        self.shards = []
        self.threads = []
        
        for i in range(0, len(data), split_size):
            
            df = data.iloc[i: i + split_size]
            
            self.shards.append( Controller("my_table_" +  str(int(i / split_size)), df) )

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