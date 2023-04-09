import pandas as pd
from util.errors import InvalidPredicate

class Table:
    
    def __init__(self, dataframe: pd.DataFrame) -> None:
        
        self.table = dataframe
        
    def query(self, column: str, predicate: str, value: int):
        
        out_df = pd.DataFrame(columns=self.table.columns)
        
        for index, row in self.table.iterrows():
            if self._compare(row[column], predicate, value):
                out_df = pd.concat([out_df, row.to_frame().T])
                
        return out_df
    
    def _compare(self, row_value: int, predicate: str, value: int):
        
        if (predicate == "LT"):
            return row_value < value
        elif (predicate == "LE"):
            return row_value <= value
        elif (predicate == "EQ"):
            return row_value == value
        elif (predicate == "NE"):
            return row_value != value
        elif (predicate == "GT"):
            return row_value > value
        elif (predicate == "GE"):
            return row_value >= value
        else:
            raise InvalidPredicate