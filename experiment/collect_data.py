import numpy as np
import pandas as pd
from star_network.StarNode import StarNode
from sharding_methods.Predicate import predicate_split
from my_statistics.StatisticsTable import StatisticsTable
from util.predicates import predicate_interval
import random

NUM_SERVERS = 5
NUM_QUERIES = 100
COLUMN = 'rating'
PRED_CHOICES = ['LT', 'LE', 'GT', 'GE']
VALUE_RANGE = [0, 10]
DATA_SIZE_SET = [100, 1000]

def main():
    
    for data_size in DATA_SIZE_SET:
        # Build Data
        ids = np.array(range(1,data_size + 1))
        ratings = np.random.rand(data_size)
        ratings = np.multiply(ratings, 10)
        df = pd.DataFrame()
        df["id"] = ids
        df["rating"] = ratings

        # Can be any split
        intervals = predicate_interval(VALUE_RANGE, NUM_SERVERS)
        frames = predicate_split(df, COLUMN, intervals)
        
        # Build 
        nodes = []
        for i in range(NUM_SERVERS):
            new_node = StarNode(i, frames[i])
            nodes.append(new_node)

        for i in range(len(nodes)):
            others = nodes[:i] + nodes[i+1:]
            nodes[i].set_connections(others)

        queries = []

        # Execute Queries
        for i in range(NUM_QUERIES):
            chosen_node = random.randint(0, NUM_SERVERS - 1)
            pred = random.choice(PRED_CHOICES)

            if 'L' in pred:
                val = (chosen_node+1) * 10 / NUM_SERVERS
            else:
                val = (chosen_node) * 10 / NUM_SERVERS
            
            # val = random.uniform(VALUE_RANGE[0], VALUE_RANGE[1])
            queries.append((chosen_node, pred, val))
            nodes[chosen_node].query(COLUMN, pred, val)
        
        # Collect the statistics
        stats = []
        for node in nodes:
            stats.append(node.stats)
        ouput_arr = StatisticsTable.combine(len(df), stats)

        # Write to files
        if data_size == 100:
            print(ouput_arr)
        
        with open(f'./data/queries_{data_size}.txt', 'w') as qf:
            for q in queries:
                qf.write(str(q[0]) + ',' + str(q[1]) + ',' + str(q[2]))
                qf.write('\n')
    
        df.to_csv(f"./data/stats_df_{data_size}.csv")
        with open(f'./data/stats_{data_size}.npy', 'wb') as f:
            np.save(f, ouput_arr)


if __name__ == "__main__":
    main()