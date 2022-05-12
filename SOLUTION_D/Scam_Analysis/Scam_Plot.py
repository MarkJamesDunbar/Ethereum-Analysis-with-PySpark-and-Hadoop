import pandas as pd
import matplotlib.pyplot as plt

# Load our output data as a pandas dataframe
data = pd.read_csv('output_d.txt', header = None, delimiter=',')

# Rename columns
data = data.rename({0: 'Category', 1:'Status', 2:'Volume (ETH)', 3:'# of Type'}, axis=1)
# Order by volume
data = data.sort_values(by='Volume (ETH)', ascending=False).reset_index(drop=True)
# Create new column
data['Avg. Volume per Type'] = data['Volume (ETH)']/data['# of Type']

# Save dataframe as a csv
data.to_csv('scams_table.csv', index=False)

# Group on status
data2 = data.groupby(['Status'], dropna=True).sum()
# Remove unnecessary column
data2 = data2.drop(['Avg. Volume per Type'], axis=1)
# Order by volume
data2 = data2.sort_values(by='Volume (ETH)', ascending=False)


# Group on category
data3 = data.groupby(['Category'], dropna=True).sum()
# Remove unnecessary column
data3 = data3.drop(['Avg. Volume per Type'], axis=1)
# Order by volume
data3 = data3.sort_values(by='Volume (ETH)', ascending=False)

# Plot the data!
data2.plot(figsize=(10,7), kind="bar")
plt.title("Volume (ETH) of Scams by Status")
plt.xticks(rotation=0)
plt.savefig('scams_stat.png')

data3.plot(figsize=(10,7), kind="bar")
plt.title("Volume (ETH) of Scams by Category")
plt.xticks(rotation=0)
plt.savefig('scams_cat.png')
