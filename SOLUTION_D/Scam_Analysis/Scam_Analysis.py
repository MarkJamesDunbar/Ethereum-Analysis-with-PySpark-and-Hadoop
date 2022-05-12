import pyspark
from operator import add
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

Scams Table
0 - id
1 - name
2 - url
3 - coin
4 - category
5 - sub-category
6 - address
7 - status
"""
# Define the context for the spark process
sc = pyspark.SparkContext()

# Define a function to check the well-formedness of the transactional and scam data
def well_formedness(l):
    # Use a try/except to filter out malformed lines
    try:
        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # If there are 7 fields in the list, it is a well formed transaction line
        if len(fields) == 7:

            # Make sure the transaction address can be converted to a string
            str(fields[2])

            # If the value of the transaction is 0, filter it out
            if int(fields[3]) == 0:
                return False

        # If there are 8 fields in the list, it is a well formed scam line
        elif len(fields) == 8:

            # Make sure the scam address can be converted to a string
            str(fields[6])

            # Make sure the scam category can be converted to a string
            str(fields[4])

            # Make sure the scam status can be converted to a string
            str(fields[7])

        # If the field length is not correct, filter it out by returning False
        else:
            return False
        # If everything is okay, return True
        return True

    except:
        # If the line is malformed, return False
        return False

def extract_transaction_data(l):
    # Use a try/except to filter out malformed lines
    try:
        # Split the fields into a list on the delimiter ','
        fields = l.split(',')

        # Make sure the transaction address can be converted to a string
        address = str(fields[2])

        # Make sure the transaction value can be converted to an ethereum float
        wei = int(fields[3])

        # Yield the address as the key, and the data as the values
        return (address, (wei, 1))
    #except Exception as e:
    except:
        # If the line is malformed, return the exception (debugging purposes)
        #return e
        pass

# Define the path to our data - both contracts and transactions
transaction_data = sc.textFile('/data/ethereum/transactions')
scam_data = sc.textFile('/user/mjd02/input/scams.csv')

#######################
# Run our spark steps #
#######################
# Apply our well-formedness filter to the transaction data
step_1 = transaction_data.filter(well_formedness)
# Apply our well-formedness filter to the scam data
step_2 = scam_data.filter(well_formedness)
# Get the scam address, status and category
step_3 = step_2.map(lambda x: (x.split(',')[6], (x.split(',')[7], x.split(',')[4])))
# Get the transaction address, and mont/value/count as a key-value pair from the data
step_4 = step_1.map(extract_transaction_data)
# Join the transaction and scam on address
step_5 = step_4.join(step_3)
# step5 = ( address, ( ( ETH, COUNT ) , ( STATUS , TYPE ) ) )
# Reformat our scams to give us uniqu scam type/status combos and their values/counts
step_6 = step_5.map(lambda x: ((x[1][1][1], x[1][1][0]), (x[1][0][0], x[1][0][1])))
# On our unique fields, sum the ethereum values and count for unique scap types and statuses
step_7 = step_6.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
# Format the output line string from the step 7 results
step_8 = step_7.map(lambda x: '{0},{1},{2},{3}'.format(x[0][0], x[0][1], x[1][0], x[1][1]))
# Save the output
step_8.saveAsTextFile('output')

# We want an output of the form SCAM_TYPE, STATE, VOLUME

######## BELOW WAS USED FOR DEBUGGING/VIEWING PIPELINE OUTPUTS
# This prints a line from each step, allowing us to see our output
# g = step_5.take(1)
# print(g)
# g = g[0]
# print(g[1][1][1])    # SCAM TYPE
# print(g[1][1][0])    # SCAM STATUS
# print(g[1][0][0])    # VALUE
# print(g[1][0][1])    # COUNT

# This shows a single line's output from a given step
#print(step_8.take(5))
