import pyspark
import time

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

            # Make sure the block difficulty can be converted to an integer
            int(fields[3])

            int(fields[7])

        # If the field length is not correct, filter it out by returning False
        else:
            return False
        # If everything is okay, return True
        return True

    except:
        # If the line is malformed, return False
        return False

# Define a function to extract contract data as key/value pairs.
def extract_block_data(l):

    # Use a try/except to filter out malformed lines
    try:
        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # Get the block number
        block_difficulty = int(fields[3])

        time_epoch = int(fields[7])

        # Get the month and year
        date = time.strftime("%m-%Y",time.gmtime(time_epoch))

        return(date, (block_difficulty, 1))

    except:
        # If the line is malformed, pass
        pass

# Define a filter function to remove None values
def remove_none(l):
    # Use a try/except to filter out malformed lines
    try:
        if l != None:
            return True
        else:
            return False
    except:
        # If the line is malformed, pass
        return False

# Define the path to our block data
block_data = sc.textFile('/data/ethereum/blocks')

#######################
# Run our spark steps #
#######################
# Apply our well-formedness filter to the block data
step_1 = block_data.filter(well_formedness)
# Get the block price and gas used from the transaction data
step_2 = block_data.map(extract_block_data)
# Remove the header artefact
step_3 = step_2.filter(remove_none)
# Get the total gas used and the number of blocks of that size by reducing on key
step_4 = step_3.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
# Get the average gas used for that block size
step_5 = step_4.mapValues(lambda x: x[0]/x[1])
# Reformat the output
step_6 = step_5.map(lambda x: '{0},{1}'.format(x[0], x[1]))
print(step_6.take(1))
# Save the output
step_6.saveAsTextFile('output_3')
