from pyspark.sql import SparkSession
from pyspark.sql import Row

sparkSessn=SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields=line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("UTF-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines=sparkSessn.sparkContext.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/fakefriends.csv")
people=lines.map(mapper) # creates the rdd of each row in the csv file

schemaPeople=sparkSessn.createDataFrame(people).cache() # creates a dataframe out of the rdd and then caches that dataframe in memory for faster access

schemaPeople.createOrReplaceTempView("people_vw")

teenagers=sparkSessn.sql("select * from people_vw where age>=13 and age<=19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

sparkSessn.stop()
