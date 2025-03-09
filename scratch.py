from pyspark.sql import SparkSession
from pyspark.sql import Row

sparkSessn=SparkSession.builder.appName("SparkSQL").getOrCreate()

def parseLine(line):
    fields=line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("UTF-8")), age=int(fields[2]), numFriends=int(fields[3]))


lines=sparkSessn.sparkContext.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/fakefriends.csv")

people=lines.map(parseLine)

peopleDF=sparkSessn.createDataFrame(people).cache()

peopleDF.createOrReplaceTempView("temp_people_vw")

teenagersDF=sparkSessn.sql("select * from temp_people_vw where age>=13 and age<=19")

print(type(teenagersDF))

for result in teenagersDF.collect():
    print(result)

peopleDF.groupBy("age").count().orderBy("count", ascending=False).show(peopleDF.count())

countByAgeDF=sparkSessn.sql("select age, count(*) from temp_people_vw group by 1 order by 2 desc")

for result in countByAgeDF.collect():
    print(result)

sparkSessn.stop()
