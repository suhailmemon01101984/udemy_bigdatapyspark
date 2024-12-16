from pyspark import SparkConf, SparkContext

sparkconfig=SparkConf().setMaster("local").setAppName("WordCount")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

input=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/book")
words=input.flatMap(lambda x: x.split())
wordCounts=words.countByValue()
for word,count in wordCounts.items():
    cleanword=word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword, count)
quit()
