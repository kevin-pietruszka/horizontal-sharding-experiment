import pandas as pd
from queue import Queue
from util.table import Table
from network_transfer.Bandwidth import Bandwidth
from my_statistics.StatisticsTable import StatisticsTable

outputs = Queue()

class Worker:
    
    def __init__(self, id:int, df: pd.DataFrame, transfer_speed: float = 1):
        
        self.id = id
        self.queries = Queue()       
        self.table = Table(df)
        self.connection = Bandwidth(transfer_speed)
        self.stats = StatisticsTable()

    def listen(self):
        
        while(True):
            query = self.queries.get()
            
            if (query == "done"):
                break
            
            col, pred, val = query
            
            output = self.table.query(col, pred, val)
            self.stats.update(output)

            self.connection.send_df(output)
            outputs.put(output)
        
    def add_query(self, query):
        self.queries.put(query)

    def __str__(self) -> str:
        return str(self.table)
    