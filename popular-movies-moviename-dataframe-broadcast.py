from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, LongType, StructType, StructField

def loadMovieNames():
    movieNames={}
    #previously i wrote the with.open command without the encoding and errors section but then it failed that default encoding of utf-8 doesn't work for this file. so i had to add the ISO encoding section to read the file correctly
    with open('/datafiles/ml-100k/u.item', 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields=line.split('|')
            movieNames[int(fields[0])]=fields[1]
    return movieNames


sparkSessn=SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict=sparkSessn.sparkContext.broadcast(loadMovieNames())

#print(type(nameDict))

movieSchema= StructType([StructField("userID", IntegerType(), True), \
                         StructField("movieID", IntegerType(), True), \
                         StructField("rating", IntegerType(), True), \
                         StructField("timestamp", IntegerType(), True)])

moviesDF=sparkSessn.read.option("sep", "\t").schema(movieSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/ml-100k/u.data")

movieCounts=moviesDF.groupBy("movieID").count().withColumnRenamed("count", "ratings_count")

def lookupMovieName(id_of_the_movie):
    return nameDict.value[id_of_the_movie]

lookupMovieNameUDF=func.udf(lookupMovieName)

movieCountsWithMovieNames=movieCounts.withColumn("movieTitle", lookupMovieNameUDF(func.col("movieID")))

#movieCountsWithMovieNames.printSchema()

topMovies=movieCountsWithMovieNames.orderBy(func.desc("ratings_count"))

#If False, it ensures that the full content of each column is displayed, without truncating the text. By default, PySpark truncates long column values (especially for string columns) to fit the display width. Passing False prevents that truncation
topMovies.show(10, False)

sparkSessn.stop()
