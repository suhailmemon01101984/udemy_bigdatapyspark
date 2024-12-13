from pyspark import SparkConf, SparkContext
import collections

sparkconfig=SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc=SparkContext(conf=sparkconfig)
#sc.setLogLevel("ERROR")
sc.setLogLevel("ERROR")
lines=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/ml-100k/u.data")
ratings=lines.map(lambda x:x.split()[2])
result=ratings.countByValue()
sortedResults=collections.OrderedDict(sorted(result.items()))
for key,value in sortedResults.items():
    print(f"{key} {value}")
quit()
