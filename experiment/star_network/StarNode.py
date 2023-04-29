from queue import Queue
from threading import Thread
from util.table import Table
import pandas as pd
from collections.abc import Sequence
from util.errors import NoConnections
from util.predicates import query_in_interval
from network_transfer.Bandwidth import Bandwidth
from my_statistics.StatisticsTable import StatisticsTable
from typing import Union
import sympy

class StarNode:
    
    def __init__(self, id:int, df:pd.DataFrame, transfer_speed: float = 1, interval: Union[sympy.Interval, None] = None) -> None:
        self.table = Table(df)
        self.id = id
        self.connections = None
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
    
    def set_connections(self, others: Sequence['StarNode']):
        self.connections = others

    def query(self, column: str, predicate: str, value: int) -> pd.DataFrame:
        """Execute query on all nodes and return result"""
        if self.connections == None:
            raise NoConnections

        connection_outputs = Queue()
        for connection in self.connections:
            x = Thread(target=connection._query, args=(connection_outputs, column, predicate, value))
            x.start()

        # Execute Query on this device
        output_df = self.table.query(column, predicate, value)
        
        # Combine output
        for i in range(len(self.connections)):
            res = connection_outputs.get()
            output_df = pd.concat([output_df, res])

        self.stats.update(output_df)
        return output_df

    def _query(self, output_queue: Queue, column: str, predicate: str, value: int):
        if query_in_interval(predicate, value, self.interval):
            output = self.table.query(column, predicate, value)
            self.connection.send_df(output)
        else:
            output = pd.DataFrame(columns=self.columns)
            self.connection.send_df(output)
        output_queue.put(output)
