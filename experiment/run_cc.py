
import numpy as np
import pandas as pd
from commander_controller.Commander import Commander

def main():
    
    ids = np.array(range(1,101))
    ratings = np.random.rand(100)
    ratings = np.multiply(ratings, 10)

    df = pd.DataFrame()
    df["id"] = ids
    df["rating"] = ratings

    client = Commander(df, num_splits=5)
    client.start_threads()

    query = ('id', 'GT', 90)
    output = client.execute_query(query)
    print(output)

    client.execute_query("done")
    
if __name__ == "__main__":
    main()