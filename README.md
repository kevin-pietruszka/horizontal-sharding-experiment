# Horizontal Sharding Network and Storage Optimization Simulator
This project explores horizontal sharding techniques and network configurations to optimize database performance and efficiency.

## Features
- Python implementation using Numpy, Pandas, Sympy, and Threads
- Simulated SQL table on different shards with configurable parameters
- Network architectures:
  - Star Network: Each node has references to all other nodes
  - Ring Network: One-way ring with next pointers
  - Controller-Worker: Controller has references to all shards
- Sharding methods:
  - Even split: Sequential split into even sizes
  - Predicate sharding: Uses known predicate preferences
  - Redistribute Algorithm: Frequency-based optimization (see algorithm in source code)

## Dependencies 
All dependencies are listed in requirements.txt file and can be install with `pip install -r requirements.txt`.

## Usage
To run any of the simulations, use python to run one of the files of the format "run_*" in the experiment folder.
