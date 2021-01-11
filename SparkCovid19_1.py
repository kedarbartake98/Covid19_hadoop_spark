from pyspark import SparkContext, SparkConf
from datetime import datetime as dt 
import sys

'''

Use MapReduce paradigm on pseudo-distributed spark to count number of cases 
per country in a certain date  range given the record of number of cases per day 
for each country in csv

'''

if __name__=='__main__':

	input_path = sys.argv[1]
	start_date = dt.strptime(sys.argv[2], "%Y-%m-%d")
	end_date = dt.strptime(sys.argv[3], "%Y-%m-%d")
	output_path = sys.argv[4]

	sc = SparkContext.getOrCreate()
	st_date = sc.broadcast(start_date)
	end_date = sc.broadcast(end_date)

	# Read csv Data
	df = sc.textFile(input_path)
	df = df.map(lambda x: x.split(','))
	header = df.first()
	# filter records and apply map
	df = df.filter(lambda x: x!=header and (st_date.value<=dt.strptime(x[0],"%Y-%m-%d")) and (dt.strptime(x[0],"%Y-%m-%d")>=end_date.value))
	df = df.map(lambda x:(x[1], int(x[3])))

	# Aggregate map key value pairs using sum function
	df = df.reduceByKey(lambda a,b:a+b)

	df.saveAsTextFile(output_path)