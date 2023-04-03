import pandas as pd
import duckdb
from queue import Queue
from threading import Thread, Lock

outputs = Queue()
query_lock = Lock()

class db_shard:
    
    def __init__(self, id:str, df: pd.DataFrame):
        
        self.id = id
        self.df = df
        self.queries = Queue()
        
        self.cond = True
        self.listener = Thread(target = self.listen, args = ())

        create_table = f"CREATE TABLE {id} AS SELECT * FROM df;"
        duckdb.sql(create_table)

    def listen(self):
        
        print(f"{self.id} started.")
        
        while(True):
            query = self.queries.get()
            print(f"{self.id} received query: {query}")
            
            if (query == "done"):
                break
            
            query_lock.acquire()
            out = duckdb.query(query).df()
            query_lock.release()
            outputs.put(out)
            
        print(f"{self.id} ended.")
        
    def add_query(self, query):
        
        query = query.replace("database", self.id)
        self.queries.put(query)

        
    def __str__(self) -> str:
        return str(self.df)