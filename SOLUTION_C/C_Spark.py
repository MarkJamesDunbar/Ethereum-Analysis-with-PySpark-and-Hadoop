import pyspark
from operator import add

"""
Blocks Table
0 - number
1 - hash
2 - miner
3 - difficulty
4 - size
5 - gas_limit
6 - gas_used
7 - timestamp (epoch)
8 - transaction_count
"""

# Define the context for the spark process
sc = pyspark.SparkContext()

# Define a function to check the well-formedness of the block data
def well_formedness(l):
    # Use a try/except to filter out malformed lines
    try:
        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # If there are 9 fields in the list, it is a well formed block line
        if len(fields) == 9:

            # If the size of the blocks mined is 0, filter it out
            if int(fields[4]) == 0:
                # Filter lines out by returning False
                return False
        else:
            # If the field length is not correct, filter it out by returning False
            return False
        # If everything is okay, return True
        return True

    except:
        # If the line is malformed, return False
        return False

# Define a function to map our data features to
def extract_block_data(l):
    # Again, use a try/except to filter out malformed lines
    try:
        # Split the fields into a list on the delimiter ','
        fields = l.split(',')

        # If the length of the line is as expected
        if len(fields) == 9:

            # Get the miner ID
            miner = str(fields[2])

            # Get the block size mined
            block_size = int(fields[4])

            # Return them as a key-value like pair
            return (miner, block_size)
    except:
        pass

# Define the path to our data
data = sc.textFile('/data/ethereum/blocks')

#######################
# Run our spark steps #
#######################
# Apply our well-formedness filter
step_1 = data.filter(well_formedness)
# First, extract the data we need
step_2 = step_1.map(extract_block_data)
# Sum the block size mined per miner
step_3 = step_2.reduceByKey(add)
# Get the ordered list of top 10 miners by block size mined
step_4 = step_3.takeOrdered(10, key=lambda x: -x[1])

# This prints a line from each step, allowing us to see our output
#print(step_4.take(1))

# Print our top 10 results
for miner in step_4:
    print('{0},{1}'.format(miner[0],miner[1]))
