import pandas as pd
import numpy as np
from commander_controller.db_shard import db_shard, outputs
import threading
import sys

class db_controller:
    
    def __init__(self, num_splits=5) -> None:
        
        ids = np.array(range(1,101))
        data = np.random.rand(100)
        data = np.multiply(data, 10)
        
        self.num_splits = num_splits
        split_size = int(len(data) / self.num_splits)
        
        self.shards = []
        self.threads = []
        
        for i in range(0, len(ids), split_size):
            
            id_split = ids[i:i+split_size]
            data_split = data[i:i+split_size]
            
            df = pd.DataFrame()
            df["id"] = id_split
            df["rating"] = data_split
            
            self.shards.append( db_shard("my_table_" +  str(int(i / split_size)), df) )

    def execute_query(self, query:str) -> pd.DataFrame:
        
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
        
if __name__ == "__main__":
    client = db_controller()
    client.start_threads()
    
    output = client.execute_query("SELECT * FROM database;")
    
    print(output.info())
    client.execute_query("done")
    
    sys.exit()