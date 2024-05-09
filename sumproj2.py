import pandas as pd
import sys
import hashlib

# Load Data
def load_data():
    # Load TSV files as pandas DataFrames, handling bad lines by skipping them.
    nodes = pd.read_csv('nodes_test.tsv', sep='\t', on_bad_lines='skip')
    edges = pd.read_csv('edges_test.tsv', sep='\t', on_bad_lines='skip')
    return nodes, edges

# MapReduce function to process data
def map_reduce(dataframe, key_column, filter_condition, filter_column):
    # Map phase
    mapped = [(row[key_column], 1) for index, row in dataframe.iterrows() if row[filter_column] == filter_condition]
    # Reduce phase
    reduced = {}
    for key, value in mapped:
        if key in reduced:
            reduced[key] += value
        else:
            reduced[key] = value
    # Sort the data by count in descending order and return the top 5 entries.
    sorted_data = sorted(reduced.items(), key=lambda x: x[1], reverse=True)
    return sorted_data[:5]

# Function to compute a stable integer hash from a string input
def stable_int_hash(value):
    # Uses SHA-256 to hash the input value (NEEDED HELP FROM CHATGPT ON THIS)
    return int(hashlib.sha256(value.encode('utf-8')).hexdigest(), 16) % (10**8)

# Mid-square hashing method
def mid_square_hash(value, r):
    # Convert the input into a stable integer hash.
    int_value = stable_int_hash(value)
    # Square the integer value.
    square = int_value ** 2
    # Convert the square into a string 
    square_str = str(square).zfill(8)
    # Calculate the middle index and extract 'r' digits 
    mid = len(square_str) // 2
    return int(square_str[mid-r:mid+r])

# Function to perform hash experiments
def hash_experiment(ids, counts):
    # Define hash methods with different values of 'r'.
    hash_methods = {
        'Mid-square r=3': lambda x: mid_square_hash(x, 3),
        'Mid-square r=4': lambda x: mid_square_hash(x, 4),
    }
    
    results = {}
    for name, func in hash_methods.items():
        hash_tables = []
        print(f"Hash Tables for {name}:")
        for i in range(10):
            table = {}
            # Apply hash function to each ID and accumulate results in tables.
            for id in ids:
                hash = func(id)
                if hash in table:
                    table[hash].append(id)
                else:
                    table[hash] = [id]
            hash_tables.append(table)
            print(f"Table {i+1}: {table}")
        # Calculate the total memory used by the tables.
        total_size = sum(sys.getsizeof(table) for table in hash_tables)
        results[name] = total_size
        print(f"Total memory used by {name}: {total_size} bytes")
    
    return results

def main():
    # Load the dataset.
    nodes, edges = load_data()
    
    # Apply MapReduce to compute gene bindings and upregulations.
    cbg_count = map_reduce(edges, 'source', 'CbG', 'metaedge')
    dug_count = map_reduce(edges, 'target', 'DuG', 'metaedge')
    
    # Output the top 5 results for CbG and DuG.
    print("Top 5 compounds with most gene bindings:")
    for compound, count in cbg_count:
        print(f"{compound}: {count}")
    
    print("\nTop 5 diseases with most upregulated genes:")
    for disease, count in dug_count:
        print(f"{disease}: {count}")
    
    # Perform hashing experiments on the results of MapReduce.
    experiment_ids = [x[0] for x in cbg_count + dug_count]
    experiment_counts = {x[0]: x[1] for x in cbg_count + dug_count}
    hash_experiment(experiment_ids, experiment_counts)

if __name__ == "__main__":
    main()
