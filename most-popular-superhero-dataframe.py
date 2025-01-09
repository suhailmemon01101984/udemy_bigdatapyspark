from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

sparkSessn=SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

marvelNameSchema=StructType([StructField("id", IntegerType(), True), \
                            StructField("name", StringType(), True)
                            ])

marvelNames=sparkSessn.read.schema(marvelNameSchema).option("sep", " ").csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/marvel-names")

marvelNames.show()

print(type(marvelNames))

lines=sparkSessn.read.text("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/marvel-graph")

print(type(lines))

lines.show()
superheroConnectionsStage=lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " "))-1)

superheroConnectionsStage.show()

print(type(superheroConnectionsStage))

superheroConnections=superheroConnectionsStage.groupBy("id").agg(func.sum("connections").alias("totalConnections")).orderBy(func.desc("totalConnections"))

superheroConnections.show()

mostPopularSuperhero=superheroConnections.first()

print(type(mostPopularSuperhero)) #note that this prints <class 'pyspark.sql.types.Row'> instead of python dataframe. on row types, the .show() doesn't work

mostPopularName=marvelNames.filter(func.col("id")==mostPopularSuperhero[0]).first()

print(type(mostPopularName)) #note that this prints <class 'pyspark.sql.types.Row'> instead of python dataframe. on row types, the .show() doesn't work

print(f"{mostPopularName[1]} is the most popular superhero with {mostPopularSuperhero[1]} co-appearances")

sparkSessn.stop()
