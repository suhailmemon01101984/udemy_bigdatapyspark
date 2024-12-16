import re

from pyspark import SparkConf, SparkContext

sparkconfig=SparkConf().setMaster("local").setAppName("WordCount")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

input=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/book")
words=input.flatMap(normalizeWords)
wordCounts=words.countByValue()
for word,count in wordCounts.items():
    cleanword=word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword.decode(), count) #this .decode() was added to change from binary format to string...so eg: from  b'mindful' to just mindful
quit()
