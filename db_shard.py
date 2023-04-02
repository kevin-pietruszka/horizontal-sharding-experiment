import pandas as pd
import duckdb

class db_shard:
    
    def __init__(self, id:str, df: pd.DataFrame):
        
        self.id = id
        self.df = df
        
        create_table = f"CREATE TABLE {id} AS SELECT * FROM df;"
        duckdb.sql(create_table)
        
    def __str__(self) -> str:
        return str(self.df)