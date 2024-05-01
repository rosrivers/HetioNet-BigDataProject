#below will be the code for question 1, it had outputted the dataframes and saved them to new files since there was a bad line in the nodes_test.tsv file and the edges_test.tsv file

import pandas as pd

nodes_df = pd.read_csv('nodes_test.tsv', sep='\t', on_bad_lines='skip') #this will skip the bad lines
edges_df = pd.read_csv('edges_test.tsv', sep='\t', on_bad_lines='skip')

print("Nodes DataFrame:")
print(nodes_df.head())
print("Edges DataFrame:")
print(edges_df.head())

nodes_df.to_csv('nodes_output.tsv', sep='\t', index=False) #this will save the data to a new file called nodes_output.tsv and do the same for edges
edges_df.to_csv('edges_output.tsv', sep='\t', index=False)

print("Data has been processed and saved.") #just to check that the data has been processed and saved -- acts as a reminder for us

#below will be the code for question 2

# Filter for edges that represent 'CbG' (Compound binds Gene)
cbg_edges = edges_df[edges_df['metaedge'] == 'CbG']

# Implementing a simple MapReduce
# Map phase: Map each compound to a count of 1 for each binding
map_result = cbg_edges['source'].value_counts().reset_index()
map_result.columns = ['Compound', 'GeneCount']

# Get the top 5 compounds with the most gene bindings
top_5_compounds = map_result.sort_values(by='GeneCount', ascending=False).head(5)

print("Top 5 compounds with the most gene bindings:")
print(top_5_compounds)

# Save the top 5 results to a file
top_5_compounds.to_csv('top_5_compounds.tsv', sep='\t', index=False)

print("Data has been processed and saved.") #again the same thing as before