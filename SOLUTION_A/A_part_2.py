from mrjob.job import MRJob
import time

'''
Transactions Table
0 - block_number
1 - from_address
2 - to_address
3 - value
4 - gas
5 - gas_price
6 - block_timestamp [epoch time]
delim = ,
'''

class A_part_2(MRJob):

    # Mapper
    def mapper(self, _, line):

        # Get only well-formed lines, ignore all else with pass.
        try:
            # Separate line into a list
            fields = line.split(',')

            # Make sure the lines are the right length
            if len(fields) == 7:

                # Get the time in epoch
                time_epoch = int(fields[6])

                # Transform wei into etherium
                value = float(fields[3])/1000000000000000000

                # Get the month and year
                month_year = time.strftime('%m,%Y', time.gmtime(time_epoch))

                yield (month_year, {'count': 1, 'value': value})
        except:
            pass


    # Combiner
    def combiner(self, key, value):
        # Define counters for the total transaction values,
        # And the total number of transactions
        total_value = float(0)
        count = 0

        for transaction in value:
            # Update the value and the count for that month
            total_value += transaction['value']
            count += transaction['count']

        result = { 'count': count, 'value': total_value }

        yield (key, result)

    # Reducer
    def reducer(self, key, value):
        # Define counters for the total transaction values,
        total_value = float(0)

        # And the total number of transactions
        count = 0


        for transaction in value:

            # Update the value counter
            total_value += transaction['value']

            # Update the transaction counter
            count += transaction['count']

        # Calculate the average transaction value
        average = total_value/count

        yield (key, average)


if __name__ == '__main__':
    A_part_2.JOBCONF = {'mapreduce.job.reduces': '4'}
    A_part_2.run()
