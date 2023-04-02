import pandas as pd
import duckdb
import queue
from threading import Thread
from dp_controller import output_queue

class db_shard:
    
    def __init__(self, id:str, df: pd.DataFrame):
        
        self.id = id
        self.df = df
        self.q_queue = queue.Queue()
        self.cond = True
        self.listener = Thread(target = listen, args = ())


        create_table = f"CREATE TABLE {id} AS SELECT * FROM df;"
        duckdb.sql(create_table)



    def listen(self):
        while(True):
            query, output = self.q_queue.get()
            output.put(duckdb.sql(query).df())
            self.q_queue.task_done()


    def make_query(query:str, Queue:output) -> None:
        self.q_queue.put((query, output))


    def forward(self, node:db_shard):


        
    def __str__(self) -> str:
        return str(self.df)