import pandas as pd
import numpy as np
from db_shard import db_shard
import queue

class db_controller:
    
    output_queue = queue.Queue()
    def __init__(self) -> None:
        
        ids = np.array(range(1,101))
        data = np.random.rand(100)
        data = np.multiply(data, 10)
        
        num_splits = 5
        split_size = int(len(data) / num_splits)
        
        shards = []
        
        for i in range(0, len(ids), split_size):
            
            id_split = ids[i:i+split_size]
            data_split = data[i:i+split_size]
            
            df = pd.DataFrame()
            df["id"] = id_split
            df["rating"] = data_split
            
            shards.append( db_shard("my_table_" +  str(int(i / split_size)), df) )

    def execute_query(self, query:str, outputs:Queue) -> Queue:
        for shard in shards:
            shard.make_query(query, outputs)
        
        return outputs

    def build_output(outputs:Queue) -> pandas.DataFrame:
        df = []
        for i in range(num_splits):
            df.append(outputs.get())
            outputs.task_done()
        return pandas.DataFrame(df)





if __name__ == "__main__":
    db_controller()