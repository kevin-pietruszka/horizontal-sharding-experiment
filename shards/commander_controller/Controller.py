import pandas as pd
from queue import Queue
from threading import Thread
from util.table import Table

outputs = Queue()

class Controller:
    
    def __init__(self, id:str, df: pd.DataFrame):
        
        self.id = id
        self.queries = Queue()
        self.listener = Thread(target = self.listen, args = ())
        
        self.table = Table(df)

    def listen(self):
        
        # print(f"{self.id} started.")
        
        while(True):
            query = self.queries.get()
            # print(f"{self.id} received query: {query}")
            
            if (query == "done"):
                break
            
            col, pred, val = query
            
            output = self.table.query(col, pred, val)
            outputs.put(output)
            
        # print(f"{self.id} ended.")
        
    def add_query(self, query):
        self.queries.put(query)

    def __str__(self) -> str:
        return str(self.table)
    