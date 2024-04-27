import pandas as pd

nodes_df = pd.read_csv('nodes_test.tsv', sep='\t', on_bad_lines='skip') #this will skip the bad lines
edges_df = pd.read_csv('edges_test.tsv', sep='\t', on_bad_lines='skip')

print("Nodes DataFrame:")
print(nodes_df.head())
print("Edges DataFrame:")
print(edges_df.head())

nodes_df.to_csv('nodes_output.tsv', sep='\t', index=False) #this will save the data to a new file called nodes_output.tsv and do the same for edges
edges_df.to_csv('edges_output.tsv', sep='\t', index=False)

print("Data has been processed and saved.") #this will print out the message "Data has been processed and saved." to the console which is just for me
