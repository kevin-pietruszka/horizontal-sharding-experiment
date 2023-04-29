import numpy as np
import pandas as pd
from star_network.StarNode import StarNode
from sharding_methods.EvenSplit import even_split
from my_statistics.StatisticsTable import StatisticsTable
import random

NUM_SERVERS = 5
SIZE = 100
NUM_QUERIES = 100
COLUMN = 'rating'
PRED_CHOICES = ['LT', 'LE', 'GT', 'GE']
VALUE_RANGE = [0, 10]

def main():
    
    ids = np.array(range(1,SIZE + 1))
    ratings = np.random.rand(SIZE)
    ratings = np.multiply(ratings, 10)

    df = pd.DataFrame()
    df["id"] = ids
    df["rating"] = ratings

    frames = even_split(df,NUM_SERVERS)
    
    nodes = []
    for i in range(NUM_SERVERS):
        new_node = StarNode(i, frames[i])
        nodes.append(new_node)

    for i in range(len(nodes)):
        others = nodes[:i] + nodes[i+1:]
        nodes[i].set_connections(others)

    chosen_node = random.randint(0, NUM_SERVERS - 1)

    
    for i in range(NUM_QUERIES):
        pred = random.choice(PRED_CHOICES)
        val = random.uniform(VALUE_RANGE[0], VALUE_RANGE[1])
        query_output = nodes[chosen_node].query(COLUMN, pred, val)
    
    stats = []

    for node in nodes:
        stats.append(node.stats)

    ouput_arr = StatisticsTable.combine(len(df), stats)
    with open('./data/stats.npy', 'wb') as f:
        df.to_csv("./data/stats_df.csv")
        np.save(f, ouput_arr)


if __name__ == "__main__":
    main()