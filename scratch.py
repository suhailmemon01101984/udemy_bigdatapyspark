from pyspark.sql import SparkSession
from pyspark.sql import functions as func

sparkSessn=SparkSession.builder.appName("WordCounts").getOrCreate()
inputDF=sparkSessn.read.text("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/book")
inputDF.printSchema()

wordsDF=inputDF.select(func.explode(func.split(inputDF.value,"\\W+")).alias("word"))

wordsDF.printSchema()

print(type(wordsDF))


wordsWithoutEmptyStringDF=wordsDF.filter(wordsDF.word!="")

lowercaseWordsDF=wordsWithoutEmptyStringDF.select(func.lower(wordsWithoutEmptyStringDF.word).alias("word"))

lowercaseWordsDF.printSchema()

wordCountsDF=lowercaseWordsDF.groupBy("word").count()

print(type(wordCountsDF))

wordCountsSortedDF=wordCountsDF.sort("count",ascending=False)

wordCountsSortedDF.show(wordCountsSortedDF.count())

sparkSessn.stop()
