
import numpy as np
import pandas as pd
from controller_worker.Controller import Controller
from sharding_methods.EvenSplit import even_split

def main():
    
    ids = np.array(range(1,101))
    ratings = np.random.rand(100)
    ratings = np.multiply(ratings, 10)

    df = pd.DataFrame()
    df["id"] = ids
    df["rating"] = ratings

    splits = even_split(df, 5)

    client = Controller(splits)
    client.start_threads()

    query = ('rating', 'GT', 8)
    output = client.execute_query(query)
    print(output)

    client.execute_query("done")
    
if __name__ == "__main__":
    main()