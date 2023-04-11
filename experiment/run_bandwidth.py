from network_transfer.Bandwidth import Bandwidth
import pandas as pd
import numpy as np
import time

def main():
    ids = np.array(range(1,100001))
    ratings = np.random.rand(100000)
    ratings = np.multiply(ratings, 10)

    df = pd.DataFrame()
    df["id"] = ids
    df["rating"] = ratings

    b = Bandwidth(1)

    start_time = time.perf_counter()
    b.send_df(df)
    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    print("Elapsed time: ", elapsed_time)
    

if __name__ == "__main__":
    main()