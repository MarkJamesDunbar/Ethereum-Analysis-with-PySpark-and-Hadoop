import pyspark
import time

"""
Transactions Table
0 - block_number
1 - from_address
2 - to_address
3 - value
4 - gas
5 - gas_price
6 - block_timestamp
"""

# Define the context for the spark process
sc = pyspark.SparkContext()

# Define a function to check the well-formedness of the transactional data
def well_formedness(l):
    # Use a try/except to filter out malformed lines
    try:
        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # If there are 7 fields in the list, it is a well formed transaction line
        if len(fields) == 7:

            # Make sure the transaction address can be converted to a string
            str(fields[2])

            # Make sure the gas price can be converted to an int
            int(fields[5])

            # Make sure the block timestamp can be converted to an int
            int(fields[6])

        # If the field length is not correct, filter it out by returning False
        else:
            return False
        # If everything is okay, return True
        return True

    except:
        # If the line is malformed, return False
        return False

# Define a function to extract transactional data as key/value pairs.
def extract_transaction_data(l):
    # Use a try/except to filter out malformed lines
    try:
        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # Get the gas price in wei
        gas_price = int(fields[5])

        # Get the time in epoch
        time_epoch = int(fields[6])

        # Get the month and year
        month_year = time.strftime("%m-%Y",time.gmtime(time_epoch))

        # Yield the date as the key, and the gas price, and count data values
        return (month_year, (gas_price, 1))

    except:
        # If the line is malformed, pass
        pass

# Define the path to our transaction data
transaction_data = sc.textFile('/data/ethereum/transactions')

#######################
# Run our spark steps #
#######################
# Apply our well-formedness filter to the transaction data
step_1 = transaction_data.filter(well_formedness)
# Get the gas price and date from the transaction data
step_2 = step_1.map(extract_transaction_data)
# Reduce by key, summing the wei values and count for all dates
step_3 = step_2.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
# Obtain the average gas price for a transaction on a specific date.
step_4 = step_3.mapValues(lambda x: float(x[0]/x[1]))
# Sort the values on their dates
step_5 = step_4.sortByKey()
# Reformat the output
step_6 = step_5.map(lambda x: '{0},{1}'.format(x[0], x[1]))
# Save the output
step_6.saveAsTextFile('output')

#print(step_5.take(1))
