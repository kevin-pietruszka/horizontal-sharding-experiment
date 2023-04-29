import pandas as pd
from queue import Queue
from util.table import Table
from network_transfer.Bandwidth import Bandwidth
from my_statistics.StatisticsTable import StatisticsTable
from util.predicates import query_in_interval
import sympy
from typing import Union

outputs = Queue()

class Worker:
    
    def __init__(self, id:int, df: pd.DataFrame, transfer_speed: float = 1, interval: Union[sympy.Interval, None] = None):

        self.id = id
        self.queries = Queue()       
        self.table = Table(df)
        self.connection = Bandwidth(transfer_speed)
        self.stats = StatisticsTable()
        self.interval = interval
        self.columns = df.columns

        if self.interval == None:
            col = df['rating']
            _min = col.min()
            _max = col.max()
            self.interval = sympy.Interval(_min, _max)
        else:
            self.interval = interval

    def listen(self):
        
        while(True):
            try:
                query = self.queries.get()
            
                if (query == "done"):
                    break
                
                col, pred, val = query
                
                if query_in_interval(pred, val, self.interval):
                    output = self.table.query(col, pred, val)
                    self.stats.update(output)
                    self.connection.send_df(output)
                else:
                    output = pd.DataFrame(columns=self.columns)
                    self.connection.send_df(output)

                outputs.put(output)
            except:
                break
        
    def add_query(self, query):
        self.queries.put(query)

    def __str__(self) -> str:
        return str(self.table)
    