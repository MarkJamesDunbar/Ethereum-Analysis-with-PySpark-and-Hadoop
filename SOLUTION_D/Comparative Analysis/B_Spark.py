import pyspark
from operator import add

"""
Transactions Table
0 - block_number
1 - from_address
2 - to_address
3 - value
4 - gas
5 - gas_price
6 - block_timestamp

Contracts Table
0 - address
1 - is_erc20
2 - is_erc721
3 - block_number
4 - timestamp (epoch)
"""
# Define the context for the spark process
sc = pyspark.SparkContext()

# Define a function to check the well-formedness of the transactional and contractual data
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

        # If there are 5 fields in the list, it is a well formed transaction line
        elif len(fields) == 5:

            # Make sure the contract address can be converted to a string
            str(fields[0])

        # If the field length is not correct, filter it out by returning False
        else:
            return False
        # If everything is okay, return True
        return True

    except:
        # If the line is malformed, return False
        return False

# Define the path to our data - both contracts and transactions
transaction_data = sc.textFile('/data/ethereum/transactions')
contract_data = sc.textFile('/data/ethereum/contracts')

#######################
# Run our spark steps #
#######################
# Apply our well-formedness filter to the transaction data
step_1 = transaction_data.filter(well_formedness)
# Apply our well-formedness filter to the contract data
step_2 = contract_data.filter(well_formedness)
# First, extract the data we need from the trsansactions
step_3 = step_1.map(lambda x: (x.split(',')[2], int(x.split(',')[3])))
# Sum the transaction values for each address
step_4 = step_3.reduceByKey(add)
# Join the transaction and contract data on the addresses
step_5 = step_4.join(step_2.map(lambda x: (x.split(',')[0], 'contract')))
# Get the ordered list of top 10 contracts by value
step_6 = step_5.takeOrdered(10, key = lambda x: -x[1][0])

# This prints a line from each step, allowing us to see our output
#print(step_6.take(1))

# Print our top 10 results, with the value in ethereum
for address in step_6:
    print("{0},{1}".format(address[0], int(address[1][0]/1000000000000000000)))
