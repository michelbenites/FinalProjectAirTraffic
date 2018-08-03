#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 04/05/2018
# Descr. : Counting the number of aircrafts (model) per hour - streaming 
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json

# Creating spark streaming context.
conf = SparkConf().setAppName("ProcessFly").setMaster("yarn")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)

# Function to parse the line.
def parse_flight_line(line):
    # Get the information that is in json format and store in dict variable.
    dict_flight = json.loads(line)  
    # Get the information from the dict variable.
    s_aircraft = dict_flight['aircraft']
    s_flight   = dict_flight['flight']
    s_airline  = dict_flight['airline']
    s_hour     = dict_flight['datetime'][:13]
    return ((s_aircraft, s_hour), s_flight)

#ssc.checkpoint("file:///home/min257/check_p")
ssc.checkpoint("hdfs:///tmp/check_p")

# Getting info from directory input
#lines = ssc.textFileStream("file:///home/min257/flights_hdfs")
lines = ssc.textFileStream("hdfs:///tmp/flights_hdfs")

# Set the log level to show only error.
sc.setLogLevel('ERROR')

# Parse the information, remove repeated values, and map rdd with the key and 1
ssc_distinct = lines.map(parse_flight_line)\
               .transform(lambda rdd: rdd.distinct())\
               .map(lambda a: (a[0], 1))

# Applies Reduce process counting the number of aircraft.
ssc_windowed = ssc_distinct.window(20, 1)\
               .reduceByKey(lambda x, y: x + y).pprint()

# Start the streaming context.
ssc.start()
ssc.awaitTermination()
ssc.stop()
