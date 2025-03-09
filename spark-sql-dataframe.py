#problem statement :take input file: fakefriends-header.csv and using pyspark dataframes, temp views produce different results (documented below in code)

from pyspark.sql import SparkSession

sparkSessn=SparkSession.builder.appName("SparkSQL").getOrCreate()

#if your file has header...you don't need to do the extra hop to rdd and then create a df out of it like in earlier example. using command below you can create df directly. this code also assumes default delimiter as comma. but if your csv file has some other delimiter eg: tab then use this code: .option("delimiter", "\t") to specify the same.
peopleDF=sparkSessn.read.option("header","true").option("inferSchema","true").csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/fakefriends-header.csv")

print(type(peopleDF))

peopleDF.printSchema()

peopleDF.select("name").show() #shows top 20 rows

peopleDF.filter(peopleDF.age<21).show() #applies age<21 filter and the shows top 20 records where age is < 21

peopleDF.groupBy("age").count().show() #takes age and does a group by on it and then shows the count by age value...top 20 rows

peopleDF.select(peopleDF.name, peopleDF.age+10).show() #selects name and adds 10 to age and then shows top 20 rows


#now doing one of the above operations with simple sql. you can do similar for others as well.

peopleDF.createOrReplaceTempView("people_header_vw")

names=sparkSessn.sql("select name from people_header_vw")
for name in names.collect():
    print(name)

sparkSessn.stop()
