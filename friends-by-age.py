#problem statement: take input file: frakefriends.csv and for each age value, calculate average number of friends for that age value
from pyspark import SparkContext, SparkConf

sparkconfig=SparkConf().setMaster("local").setAppName("FriendsByAge")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

def parseLine(line):
    fields=line.split(',')
    age=int(fields[2])
    numfriends=int(fields[3])
    return(age,numfriends)

lines=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/fakefriends.csv")
age_numfriends_key_value_rdd=lines.map(parseLine)
totalsbyage=age_numfriends_key_value_rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
averagebyage=totalsbyage.mapValues(lambda x: x[0]/x[1])
results=averagebyage.collect()
for eachrow in results:
    print(eachrow)

quit()
