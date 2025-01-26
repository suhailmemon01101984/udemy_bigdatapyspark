#problem statement: take input file: u.data and print rating and count of ratings and sort end result by rating value.
from pyspark import SparkConf, SparkContext
import collections

#create the spark config:set the master node to local machine. set your application name (good coding practice)
#create the spark context using the spark config
sparkconfig=SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc=SparkContext(conf=sparkconfig)
#sc.setLogLevel("ERROR")
#set the log level to only "error" so that spark is not that verbose in it's output showing all the warnings and stuff
sc.setLogLevel("ERROR")
#use the textfile method to. the textfile breaks up this u.data file line by line and every line corresponds to one value in the rdd.
#so if your file u.data has 5 lines then your rdd will have 5 values with each value being the string that represents the whole line of text
lines=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/ml-100k/u.data")
#call the map function with input as the lines rdd where for each line you will split that line by space and then get the values in the 3rd column
ratings=lines.map(lambda x:x.split()[2])
#call the countbyvalue function to count number of occurrences of each value
result=ratings.countByValue()
sortedResults=collections.OrderedDict(sorted(result.items()))
for key,value in sortedResults.items():
    print(f"{key} {value}")
quit()
