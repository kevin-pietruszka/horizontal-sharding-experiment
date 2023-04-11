import pandas as pd

class Bandwidth:

    def __init__(self, bandwidth: float) -> None:
        # Bandwidth is MB/s
        self.bandwidth = bandwidth * 1000000

    def transfer_speed(self, df: pd.DataFrame):
        size_of_memory = df.memory_usage(deep=True).sum()

        return size_of_memory / self.bandwidth