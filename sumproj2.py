import pandas as pd
import sys
import hashlib

# Task 1
def load_data():
    nodes_df = pd.read_csv('nodes_test.tsv', sep='\t', on_bad_lines='skip')
    edges_df = pd.read_csv('edges_test.tsv', sep='\t', on_bad_lines='skip')
    return nodes_df, edges_df

# Task 2
def compute_cbg(edges_df):
    cbg_edges = edges_df[edges_df['metaedge'].str.contains('CbG')]
    return cbg_edges['source'].value_counts().head(5)

# Task 3
def compute_dug(edges_df):
    dug_edges = edges_df[edges_df['metaedge'].str.contains('DuG')]
    return dug_edges['target'].value_counts().head(5)

# put this because for some reaosn the code would not run without it?
def stable_int_hash(value):
    return int(hashlib.sha256(value.encode('utf-8')).hexdigest(), 16) % (10**8)

#   Task 5
# Mid-square hashing method
def mid_square_hash(value, r):
    int_value = stable_int_hash(value)
    square = int_value ** 2
    square_str = str(square).zfill(8)
    mid = len(square_str) // 2
    return int(square_str[mid-r:mid+r])

def hash_experiment(ids, counts):
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
            for id in ids:
                hash_value = func(id)
                if hash_value in table:
                    table[hash_value].append(id)
                else:
                    table[hash_value] = [id]
            hash_tables.append(table)
            print(f"Table {i+1}: {table}")
        total_size = sum(sys.getsizeof(table) for table in hash_tables)
        results[name] = total_size
        print(f"Total memory used by {name}: {total_size} bytes")
    
    return results
def main():
    # Load data
    nodes_df, edges_df = load_data()
    
    # these good
    cbg_count = compute_cbg(edges_df)
    dug_count = compute_dug(edges_df)
    
    print("Top 5 compounds with most gene bindings:")
    print(cbg_count)
    print("\nTop 5 diseases with most upregulated genes:")
    print(dug_count)
    
    # dude idk whats going on here, lowkey. but it keeps printing total memory and not a hashing table which is kinda confusing me but whatevs
    example_ids = list(cbg_count.index) + list(dug_count.index)
    example_counts = {**dict(cbg_count), **dict(dug_count)}
    hash_experiment(example_ids, example_counts)

if __name__ == "__main__":
    main()
