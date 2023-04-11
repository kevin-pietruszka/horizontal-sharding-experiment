from network_transfer.Bandwidth import Bandwidth
import pandas as pd
import numpy as np

def main():
    ids = np.array(range(1,10001))
    ratings = np.random.rand(10000)
    ratings = np.multiply(ratings, 10)

    df = pd.DataFrame()
    df["id"] = ids
    df["rating"] = ratings

    b = Bandwidth(1)

    print(b.transfer_speed(df))
    

if __name__ == "__main__":
    main()