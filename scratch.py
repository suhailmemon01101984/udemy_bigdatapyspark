from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

sparkSessn=SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

marvelNamesSchema=StructType([StructField("id", IntegerType(), True), \
                              StructField("name", StringType(), True)])

marvelNamesDF=sparkSessn.read.schema(marvelNamesSchema).option("sep", " ").csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/marvel-names")

marvelNamesDF.show()

marvelGraphDF=sparkSessn.read.text("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/marvel-graph")

marvelGraphDF.show()

marvelGraphDF.printSchema()

marvelGraphIDConnDF=marvelGraphDF.withColumn("id", func.split(func.col("value"), " ")[0]).withColumn("connections", func.size(func.split(func.col("value"), " "))-1)

marvelTotalConnsByID=marvelGraphIDConnDF.groupBy("id").agg(func.sum("connections").alias("totalconnections")).sort(func.desc("totalconnections"))

marvelHeroMostPopularIDConn=marvelTotalConnsByID.first()

print(marvelHeroMostPopularIDConn[0], marvelHeroMostPopularIDConn[1])

marvelHeroMostPopularIDName=marvelNamesDF.filter(func.col("id")==marvelHeroMostPopularIDConn[0]).first()

print(marvelHeroMostPopularIDName[0], marvelHeroMostPopularIDName[1])

print(f"{marvelHeroMostPopularIDName[1]} is the most popular superhero with {marvelHeroMostPopularIDConn[1]} co-appearances")

sparkSessn.stop()
