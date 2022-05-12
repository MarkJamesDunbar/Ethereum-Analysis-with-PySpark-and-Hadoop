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

# Define a function to check the well-formedness of the transactional and scam data
def well_formedness(l):
    # Use a try/except to filter out malformed lines
    try:

        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # If there are 7 fields in the list, it is a well formed transaction line
        if len(fields) == 7:

            # Make sure the transaction value can be converted to a string
            int(fields[3])

            int(fields[6])

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
        from_address = str(fields[1])

        # Make sure the transaction address can be converted to a string
        to_address = str(fields[2])

        # Make sure the transaction value can be converted to an ethereum float
        value = int(fields[3])

        time_epoch = int(fields[6])

        # Get the month and year
        date = time.strftime("%d-%m-%Y",time.gmtime(time_epoch))

        # Yield the address as the key, and the data as the values
        return (date, (from_address, to_address, value))
    #except Exception as e:
    except:
        # If the line is malformed, return the exception (debugging purposes)
        #return e
        pass

# Define a function to extract transactional data as key/value pairs.
def remove_none(l):
    # Use a try/except to filter out malformed lines
    try:
        if l == None:
            return False
        else: return True
    except:
        return False

# Define the path to our transaction data
transaction_data = sc.textFile("/data/ethereum/transactions")

#######################
# Run our spark steps #
#######################
# Apply our well-formedness filter to the transaction data
step_1 = transaction_data.filter(well_formedness)
# getting timestamp and transaction_count
step_2 = step_1.map(extract_transaction_data)

#step 2 format: ('date', (from_addr, 'to_addr, value))
# Reduce the values by key (dd/mm/yy)
step_3 = step_2.reduceByKey(lambda x, y: (x[1][0], x[1][1], x[1][2]) if (x[1][1] == y[1][0] and x[1][0] == y[1][1] and x[1][2] == y[1][2]) else None)

# Remove any NONE values
step_4 = step_3.map(remove_none)




 
 