#problem statement: take input files: marvel names and marvel graph and figure out the name of the superhero with the least amount of connections. the hero with the lest amount of connections is the most obscure superhero

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sparkSessn=SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

marvelNameSchema=StructType([StructField("id", IntegerType(), True), \
                             StructField("name", StringType(), True) \
                             ])

marvelNames=sparkSessn.read.schema(marvelNameSchema).option("sep", " ").csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/marvel-names")

lines=sparkSessn.read.text("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/marvel-graph")

superHeroConnectionsStage=lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " "))-1)

superHeroConnectionsStage.show()

superHeroConnections=superHeroConnectionsStage.groupBy("id").agg(func.sum("connections").alias("totalConnections")).orderBy("totalConnections")

superHeroConnections.show()

superHeroSingleConnectionStage=superHeroConnections.filter(superHeroConnections.totalConnections=="1")

superHeroSingleConnectionStage.show()

superHeroSingleConnection=superHeroSingleConnectionStage.join(marvelNames,"id").select("name", "totalConnections")

superHeroSingleConnection.show(superHeroSingleConnection.count())

superHeroZeroConnectionStage=superHeroConnections.filter(superHeroConnections.totalConnections=="0")
superHeroZeroConnectionStage.show()

superHeroZeroConnection=superHeroZeroConnectionStage.join(marvelNames,"id").select("name", "totalConnections")

superHeroZeroConnection.show(superHeroZeroConnection.count())


sparkSessn.stop()
