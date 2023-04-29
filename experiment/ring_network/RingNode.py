from queue import Queue
from threading import Thread, Lock
from util.table import Table
import pandas as pd
from util.errors import NoConnections
from network_transfer.Bandwidth import Bandwidth
from my_statistics.StatisticsTable import StatisticsTable
from typing import Union
from util.predicates import query_in_interval
import sympy

ring_lock = Lock()

class RingNode:

    def __init__(self, id:int, number_of_nodes: int, df:pd.DataFrame, transfer_time:float = 1, interval: Union[sympy.Interval, None] = None) -> None:
        self.table = Table(df)
        self.id = id
        self.num_nodes = number_of_nodes
        self.next = None
        self.connection_next = Bandwidth(transfer_time)
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
    
    def set_link(self, next_node: 'RingNode'):
        self.next = next_node

    def query(self, column: str, predicate: str, value: int) -> pd.DataFrame:
        """Execute query on all nodes and return result"""
        if self.next == None:
            raise NoConnections
        
        connection_outputs = Queue()
        x = Thread(target=self.next._query, args=(self.id, connection_outputs, column, predicate, value))
        x.start()

        # Execute Query on this device
        output_df = self.table.query(column, predicate, value)

        if query_in_interval(predicate, value, self.interval):
            output_df = self.table.query(column, predicate, value)
        else:
            output_df = pd.DataFrame(columns=self.columns)
            
        # Combine output
        for i in range(self.num_nodes - 1):
            res = connection_outputs.get()
            output_df = pd.concat([output_df, res])

        self.stats.update(output_df)
        return output_df
    

    def _query(self, original_id: int, output_queue: Queue, column: str, predicate: str, value: int):

        if self.next.id != original_id:
            x = Thread(target=self.next._query, args=(original_id, output_queue, column, predicate, value))
            x.start()

        if query_in_interval(predicate, value, self.interval):
            output = self.table.query(column, predicate, value)
        else:
            output = pd.DataFrame(columns=self.columns)
        
        curr = self
        ring_lock.acquire()
        while (curr.next.id != original_id):
            curr.connection_next.send_df(output)
            curr = curr.next
        ring_lock.release()

        output_queue.put(output)