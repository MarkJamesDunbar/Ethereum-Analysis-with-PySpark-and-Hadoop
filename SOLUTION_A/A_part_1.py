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

class A_part_1(MRJob):

	# Mapper
	def mapper(self,_,line):

		try:
			# Separate line into a list
			fields = line.split(',')

			# Make sure the lines are the right length
			if len(fields) == 7:

				# Get the time in epoch
				time_epoch = int(fields[6])

				# Get the month and year
				month_year = time.strftime("%m,%Y",time.gmtime(time_epoch))

				yield(month_year, 1)
		except:
			pass

	# Combiner
	def combiner(self,key,value):
		yield(key, sum(value))

	# Reducer
	def reducer(self,key,value):
		yield(key,sum(value))


if __name__ =='__main__':
	A_part_1.JOBCONF = {'mapreduce.job.reduces': '4'}
	A_part_1.run()
