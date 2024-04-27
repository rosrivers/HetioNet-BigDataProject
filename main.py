import pandas as pd

#read the data
nodes_df = pd.read_csv('nodes_test.tsv', sep='\t')
edges_df = pd.read_csv('edges_test.tsv', sep='\t')

# print the data
print("Nodes DataFrame:")
print(nodes_df.head())
print("Edges DataFrame:")
print(edges_df.head())

nodes_df.to_csv('nodes_output.tsv', sep='\t', index=False) # Save the nodes data to a new TSV file
edges_df.to_csv('edges_output.tsv', sep='\t', index=False) # Save the edges data to a new TSV file

print("Data has been processed and saved.")


