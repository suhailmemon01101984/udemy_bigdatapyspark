from pyspark.sql import SparkSession
from pyspark.sql import functions as func

sparkSessn=SparkSession.builder.appName("FriendsByAge").getOrCreate()

peopleDF=sparkSessn.read.option("header","true").option("inferSchema","true").csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/fakefriends-header.csv")

print(type(peopleDF))

peopleDF.printSchema()

friendsAvgByAge=peopleDF.groupBy("age").avg("friends").show()

friendsAvgByAgeRounded=peopleDF.groupBy("age").agg(func.round(func.avg("friends"),2).alias("average_number_of_friends")).show()

friendsAvgByAgeRoundedSorted=peopleDF.groupBy("age").agg(func.round(func.avg("friends"),2).alias("average_number_of_friends")).sort("age", ascending=False).show(peopleDF.count())

peopleDF.createOrReplaceTempView("TempView_people_header_vw")

friendsAvgByAgeRoundedSortedSQL=sparkSessn.sql("select age, round(avg(friends),2) as average_number_of_friends from TempView_people_header_vw group by 1 order by 1 desc")

for eachRow in friendsAvgByAgeRoundedSortedSQL.collect():
    print(eachRow)

sparkSessn.stop()

