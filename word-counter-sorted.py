import re

from pyspark import SparkConf, SparkContext

sparkconfig=SparkConf().setMaster("local").setAppName("WordCount")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower()) #\W+ to remove all non word characters and use it as a splitting pattern

input=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/book")
words=input.flatMap(normalizeWords)
wordCounts=words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
wordCounts_flipped=wordCounts.map(lambda x: (x[1], x[0])) # here you are making sure that count becomes the first column (key) and the word becomes the second column
wordCountsSorted=wordCounts_flipped.sortByKey() # this will sort the data by count ascending
results=wordCountsSorted.collect()
for eachrow in results:
    cleanword=eachrow[1].encode('ascii','ignore')
    if(cleanword):
        print(f'{cleanword.decode()}:        {eachrow[0]}') #this .decode() was added to change from binary format to string...so eg: from  b'mindful' to just mindful
quit()