#problem statement: take input text file: u.data which contains userid, movieid, rating and timestamp and output most popular movies

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, LongType

sparkSessn = SparkSession.builder.appName("PopularMovies").getOrCreate()

movieSchema = StructType([StructField("userID", IntegerType(), True),
                          StructField("movieID", IntegerType(), True),
                          StructField("rating", IntegerType(), True),
                          StructField("timestamp", LongType(), True)])

moviesDF = sparkSessn.read.option("sep", "\t").schema(movieSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/ml-100k/u.data")

topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count")) # do func.desc to sort by descending order of count

topMovieIDs.show(10)

sparkSessn.stop()
