from pyspark.sql import SparkSession
from pyspark.sql import functions as func

sparkSessn=SparkSession.builder.appName("SparkSQL").getOrCreate()
peopleDF=sparkSessn.read.option("header","true").option("inferSchema","true").csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/fakefriends-header.csv")

peopleDF.printSchema()

friendsbyage=peopleDF.select(peopleDF.age,peopleDF.friends)

friendsbyage.groupBy("age").avg("friends").show()

friendsbyage.groupBy("age").avg("friends").sort("age").show()

friendsbyage.groupBy("age").agg(func.round(func.avg("friends"),2)).sort("age").show()

friendsbyage.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

#doing same code with sql after creating a view below
peopleDF.createOrReplaceTempView("people_header_vw")
avg_friends_by_age=sparkSessn.sql("select age, round(avg(friends),2) as friends_avg from people_header_vw group by 1 order by 1")
for eachrow in avg_friends_by_age.collect():
    print(eachrow[0],eachrow[1])

sparkSessn.stop()
