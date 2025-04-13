from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

sparkSessn=SparkSession.builder.appName("PopularMovies").getOrCreate()

movieSchema=StructType([StructField("userID",IntegerType(),True), \
                        StructField("movieID",IntegerType(),True), \
                        StructField("rating",IntegerType(),True), \
                        StructField("timestamp",LongType(),True) \
                        ])

moviesDF=sparkSessn.read.option("sep","\t").schema(movieSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/ml-100k/u.data")

moviesDF.printSchema()

topMoviesDF=moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

topMoviesDF.show()

sparkSessn.stop()
