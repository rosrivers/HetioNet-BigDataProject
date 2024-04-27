import pandas as pd

# Reading the nodes file
nodes_df = pd.read_csv('Node.tsv', sep='\t')
print("Nodes Data:")
print(nodes_df.head())

# Reading the edges file
edges_df = pd.read_csv('Edge.tsv', sep='\t')
print("Edges Data:")
print(edges_df.head())
