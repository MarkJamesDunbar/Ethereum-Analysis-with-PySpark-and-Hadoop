import pandas as pd
import matplotlib.pyplot as plt

# Load our output data as a pandas dataframe
data1 = pd.read_csv('output_d_1.txt', header = None, delimiter=',')

# Rename columns
data1 = data1.rename({0: 'Date', 1:'GasPricePerTransaction'}, axis=1)

# Convert dates to datetime format
data1['Date'] =  pd.to_datetime(data1['Date'], format='%m-%Y')

# Order the dates
data1 = data1.sort_values(by='Date').reset_index(drop=True)

data1['Date'] = data1['Date'].astype(str)
data1['Date'] = data1['Date'].str[:-3]


# Plot the data!
data1.set_index('Date').plot(figsize=(10,7), kind="bar")

plt.title("Average Gas Price per Transaction over Time")
plt.xlabel("Date (by Month)")
plt.xticks(rotation=60)
plt.ylabel("Average Gas price per Transaction (Wei)")
plt.legend(loc='upper right')

# Save our plot as a png
plt.savefig('Gas_Analysis_part_1.png')

# Load our output data as a pandas dataframe
data2 = pd.read_csv('output_d_3.txt', header = None, delimiter=',')

# Rename columns
data2 = data2.rename({0: 'Date', 1:'Avg. Block Difficulty'}, axis=1)

# Convert dates to datetime format
data2['Date'] =  pd.to_datetime(data2['Date'], format='%m-%Y')

# Order the dates
data2 = data2.sort_values(by='Date').reset_index(drop=True)

data2['Date'] = data2['Date'].astype(str)
data2['Date'] = data2['Date'].str[:-3]

# Plot the data!
data2.set_index('Date').plot(figsize=(20,7), kind='bar')
plt.title("Block average difficulty over time")
plt.xlabel("Date (by Month)")
plt.xticks(rotation=60)
plt.ylabel("Difficulty")
plt.savefig('Gas_Analysis_part_2.png')
