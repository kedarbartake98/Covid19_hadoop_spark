'''
Use MapReduce paradigm on pseudo-distributed hadoop to count the number of cases 
per million people country-wise. 

Input:

- CSV containing population details of each country
- CSV containing daily record of new cases and deaths country-wise

Demonstrates the use of broadcast function in Spark similar to what setup did 
in Hadoop. Country Populations were read and broadcast to all workers for fast 
access.

'''

from pyspark import SparkContext, SparkConf
from datetime import datetime as dt 
import sys

def get_pop(x):

	try:
		return (x[0], (x[1]*1000000)/country_pop.value[x[0]])
	
	except:
		pass

if __name__=='__main__':

	input_path = sys.argv[1]
	pop_path = sys.argv[2]
	output_path = sys.argv[3]

	# initialize 
	sc = SparkContext.getOrCreate()

	# create population file for broadcast
	country_df = sc.textFile(pop_path)
	country_df = country_df.map(lambda x: x.split(','))
	header = country_df.first()
	country_df = country_df.filter(lambda x: x!=header and x[-1]!='')

	country_df = country_df.map(lambda x: (x[1], int(x[-1])))
	country_pop =  country_df.collectAsMap()

	# Broadcast to all workers
	country_pop = sc.broadcast(country_pop)

	# Process actual data
	df = sc.textFile(input_path)
	df = df.map(lambda x: x.split(','))
	header = df.first()
	df = df.filter(lambda x: x!=header)

	# Apply MapReduce
	df = df.map(lambda x: (x[1], int(x[2])))
	df = df.reduceByKey(lambda a,b:a+b)		
	df = df.map(lambda x: get_pop(x))
	df = df.filter(lambda x: x is not None)

	df.saveAsTextFile(output_path)

