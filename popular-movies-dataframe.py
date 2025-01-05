from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, LongType

sparkSessn = SparkSession.builder.appName("PopularMovies").getOrCreate()

movieSchema = StructType([StructField("userID", IntegerType(), True),
                          StructField("movieID", IntegerType(), True),
                          StructField("rating", IntegerType(), True),
                          StructField("timestamp", LongType(), True)])

moviesDF = sparkSessn.read.option("sep", "\t").schema(movieSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/ml-100k/u.data")

topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

topMovieIDs.show(10)

sparkSessn.stop()
