
import numpy as np
import pandas as pd
from ring_network.RingNode import RingNode
from sharding_methods.EvenSplit import even_split

NUM_SERVERS = 10

def main():
    
    ids = np.array(range(1,10001))
    ratings = np.random.rand(10000)
    ratings = np.multiply(ratings, 10)

    df = pd.DataFrame()
    df["id"] = ids
    df["rating"] = ratings

    frames = even_split(df,NUM_SERVERS)
    
    nodes = []
    for i in range(NUM_SERVERS):
        new_node = RingNode(i, NUM_SERVERS, frames[i])
        nodes.append(new_node)

    for i in range(0, len(nodes) - 1):
        nodes[i].set_link(nodes[i+1])
    
    nodes[-1].set_link(nodes[0])

    query_output = nodes[0].query('id', 'EQ', 90)
    print(query_output)

if __name__ == "__main__":
    main()