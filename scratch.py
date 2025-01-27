from pyspark import SparkConf, SparkContext
import collections

sparkconfig=SparkConf().setMaster("local").setAppName("RatingCounter")
sc=SparkContext(conf=sparkconfig)


lines=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/ml-100k/u.data")
ratings=lines.map(lambda x:x.split()[2])
results=ratings.countByValue()
sortedresults=collections.OrderedDict(sorted(results.items()))
for key,value in sortedresults.items():
    print(key,value)
