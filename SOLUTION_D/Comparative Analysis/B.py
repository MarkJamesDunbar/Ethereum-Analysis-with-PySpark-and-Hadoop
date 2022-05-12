from mrjob.job import MRJob
from mrjob.step import MRStep

"""
Transactions Table
0 - block_number
1 - from_address
2 - to_address
3 - value
4 - gas
5 - gas_price
6 - block_timestamp
delim = ,

Contracts Table
0 - address
1 - is_erc20
2 - is_erc721
3 - block_number
4 - UTC timestamp
delim = ,
"""

# As we are going to compare this task with spark for part D of the project,
# all of the jobs listed in part B have been combined into this one file
# in order to run the entire hadoop MR task as fast as possible for a more reasonable
# comparison with the spark task

class B(MRJob):

    # We can run 2 sets of Mapper Reducer tasks by defining the steps
    # Code cited from https://mrjob.readthedocs.io/en/latest/job.html
    def steps(self):
        # Set up two MapReduce jobs to run one after the other
        return [MRStep(mapper=self.mapper_1, reducer=self.reducer_1, jobconf={'mapreduce.job.reduces': '8'}),
            MRStep(mapper=self.mapper_2, reducer=self.reducer_2, jobconf={'mapreduce.job.reduces': '10'})]


    # Mapper 1
    # This completes B jobs 1 and 2 - aggregation and join
    def mapper_1(self,_,line):
        try:
            fields = line.split(',')
            # If the line length is 5, it's from contracts
            if len(fields) == 5:

                # Get the contract address
                contract_address = fields[0]

                # Yield the contract address
                # If it's a contract, it won't have a value to output, so set this to None
                yield(contract_address, {'is_contract':True, 'value':None})

            # If the line length is 7, it's from transactions
            elif len(fields) == 7:

                # Get the transaction to address
                transaction_address = fields[2]

                # Now get the value of the contract in ethereum
                # Convert wei into etherium
                value = float(fields[3])/1000000000000000000

                # Yield the contract to_address
                # If it's a transaction, output the value
                yield(transaction_address, {'is_contract':False, 'value':value})
        except:
            pass

    # Reducer 1
    # This completes B jobs 1 and 2 - aggregation and join
    def reducer_1(self, key, values):
        # Transaction value counter
        total_value = float(0)
        contract_flag = False

        for value in values:
            # If is_contract returns False, this code runs
            if value['is_contract']:
                contract_flag = True

            elif not value['is_contract']:
                # If we have a transaction, add to the value counter
                total_value += float(value['value'])

        # If is_contract returns True, this code runs
        if contract_flag == True:
	
            # If we have a contract,
            # yield the total transactions value
            yield(key, total_value)


    def mapper_2(self, key, total_value):
        try:
            # Filter out the large number of 0-value
            # Aaddresses returned after the first reducer
            # This should speed things up in the final reducer
            if total_value > 0:
                # This yields a list, so that we can easily manipulate the contents in the reducer
                yield(None, (key,total_value))

        except:
            pass


    def reducer_2(self,key,value):
        # We order our list of addresses and their respective etherium values
        # Using the second element, which is the total etherium value
        sorted_descending = sorted(value, reverse=True, key=lambda x: x[1])
	
        for value in sorted_descending[:10]:
            yield(value[0], value[1])

if __name__ == '__main__':
    B.run()
