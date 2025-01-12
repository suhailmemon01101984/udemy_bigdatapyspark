from pyspark.sql import SparkSession
from pyspark.sql.functions import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType


sparkSessn = SparkSession.builder.appName("MovieSimilarities").getOrCreate()

movieNameSchema = StructType([StructField("movieid", IntegerType(), True) \
                              StructField("movieName", StringType(), True)])

movieSchema= StructType([StructField("userID", IntegerType(), True), \
                         StructField("movieID", IntegerType(), True), \
                         StructField("rating", IntegerType(), True), \
                         StructField("timestamp", LongType(), True)])

movieNames=sparkSessn.read.option("sep", "|").schema(movieNameSchema).csv("")





sparkSessn.stop()
