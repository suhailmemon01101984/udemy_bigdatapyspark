#problem statement: take input file: book and get all the words and calculate the count of each word in the book

import re

from pyspark import SparkConf, SparkContext

sparkconfig=SparkConf().setMaster("local").setAppName("WordCount")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower()) #re.compile(r'\W+', re.UNICODE) compiles a regular expression pattern for matching non-word characters. \W+ matches one or more non-word characters (i.e., anything that isn't a letter, number, or underscore). re.UNICODE ensures that the regex operations respect Unicode characters, making it work correctly for characters beyond the standard ASCII set. The split() function splits the text (now in lowercase) wherever the regex pattern (\W+, one or more non-word characters) matches

input=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/book")
words=input.flatMap(normalizeWords)
wordCounts=words.countByValue()
for word,count in wordCounts.items():
    cleanword=word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword.decode(), count) #this .decode() was added to change from binary format to string...so eg: from  b'mindful' to just mindful
quit()
