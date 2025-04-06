#problem statement: take input text file: book and get all the words of the book and print their counts in descending order

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

sparkSessn=SparkSession.builder.appName("WordCount").getOrCreate()
inputDF=sparkSessn.read.text("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/book")

#the below command splits the content of book using delimiter which is a regex \\W+ which means any non word characters: letter, number, underscore. once it does the split then the explode function is to convert the array words into rows and ensures each word is in it's own row
words= inputDF.select(func.explode(func.split(inputDF.value,"\\W+")).alias("word"))
wordsWithoutEmptyString=words.filter(words.word!="")
lowerCaseWords=wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))
wordCounts=lowerCaseWords.groupBy("word").count()
wordCountsSorted=wordCounts.sort("count", ascending=False)# add ,ascending=False to sort by descending. else remove this portion to show order in ascending which is the default
wordCountsSorted.show(wordCountsSorted.count()) #by default the show method will only display top 20 rows. to make it show every row you pass the parameter which is the count of the whole dataframe: wordCountsSorted

sparkSessn.stop()
