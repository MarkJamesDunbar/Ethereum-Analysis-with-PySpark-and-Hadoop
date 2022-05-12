import pyspark
import time

"""
Contracts Table
0 - address
1 - is_erc20
2 - is_erc721
3 - block_number
4 - timestamp (epoch)

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

# Define a function to check the well-formedness of the transactional data
def well_formedness(l):

    # We want to extract out top ten services for analysis
    top_ten_services = ["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444",
                        "0xfa52274dd61e1643d2205169732f29114bc240b3",
                        "0x7727e5113d1d161373623e5f49fd568b4f543a9e",
                        "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef",
                        "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8",
                        "0xbfc39b6f805a9e40e77291aff27aee3c96915bdd",
                        "0xe94b04a0fed112f3664e45adb2b8915693dd5ff3",
                        "0xbb9bc244d798123fde783fcc1c72d3bb8c189413",
                        "0xabbb6bebfa05aa13e908eaa492bd7a8343760477",
                        "0x341e790174e3a4d35b65fdc067b6b5634a61caea"]

    # Use a try/except to filter out malformed lines
    try:
        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # If there are 5 fields in the list, it is a well formed contract line
        if len(fields) == 5:

            # Make sure the contract block number can be converted to a string
            str(fields[3])

            # Make sure the contract address can be converted to a string
            address = str(fields[0])

            # Filter the addresses based on the top ten addresses
            if address in top_ten_services:
                return True
            else:
                return False

        # If there are 9 fields in the list, it is a well formed blocks line
        elif len(fields) == 9:

            # Make sure the block size can be converted to an int
            int(fields[4])

            # Make sure the block gas used can be converted to an int
            int(fields[6])

            # Make sure the block number can be converted to a string
            str(fields[0])

        # If the field length is not correct, filter it out by returning False
        else:
            return False
        # If everything is okay, return True
        return True

    except:
        # If the line is malformed, return False
        return False

# Define a function to extract contract data as key/value pairs.
def extract_contract_data(l):

    # Use a try/except to filter out malformed lines
    try:
        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # Get the block number
        block_number = fields[3]

        # Get the address
        address = str(fields[0])

        return(block_number, address)

    except:
        # If the line is malformed, pass
        pass

# Define a function to extract block data as key/value pairs.
def extract_block_data(l):
    # Use a try/except to filter out malformed lines
    try:
        # Split the lines into a list of fields on the deliimiter ','
        fields = l.split(',')

        # Get the block size
        block_size = fields[4]

        # Get the gas used
        gas_used = fields[6]

        # Get the block number
        block_number = fields[0]

        # Return the block number, and blocksize/gas used as a key/value pair
        return(block_number, (block_size, gas_used))

    except:
        # If the line is malformed, pass
        pass

# Define the path to our block and contract data
contract_data = sc.textFile('/data/ethereum/contracts')
block_data = sc.textFile('/data/ethereum/blocks')

#######################
# Run our spark steps #
#######################
# Apply our well-formedness filter to the contract data
step_1 = contract_data.filter(well_formedness)
# Apply our well-formedness filter to the block data
step_2 = block_data.filter(well_formedness)
step_3 = step_1.map(extract_contract_data)
step_4 = step_2.map(extract_block_data)
step_5 = step_4.join(step_3)
# step5 = ( block_number, (( block_size, gas_used ), address))
# Reformat our key/value pairs
step_6 = step_5.map(lambda x: (x[0], (x[1][1], x[1][0][0], x[1][0][1])))
# Reformat our output
step_7 = step_6.map(lambda x: '{0},{1},{2},{3}'.format(x[0], x[1][0], x[1][1], x[1][2]))

# Save the output
step_7.saveAsTextFile('output_2')
