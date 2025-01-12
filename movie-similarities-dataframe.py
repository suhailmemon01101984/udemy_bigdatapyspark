from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

def computeCosineSimilarity(data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2"))

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2", "movie1Name", "movie2Name") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs")
      )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "movie1Name", "movie2Name", "score", "numPairs")

    return result

sparkSessn = SparkSession.builder.appName("MovieSimilarities").getOrCreate()

movieNameSchema = StructType([StructField("movieid", IntegerType(), True), \
                              StructField("movieName", StringType(), True)])

movieSchema= StructType([StructField("userID", IntegerType(), True), \
                         StructField("movieID", IntegerType(), True), \
                         StructField("rating", IntegerType(), True), \
                         StructField("timestamp", LongType(), True)])

movieNames=sparkSessn.read.option("sep", "|").schema(movieNameSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/ml-100k/u.item")
movies=sparkSessn.read.option("sep", "\t").schema(movieSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/ml-100k/u.data")

#movieNames.show()
#movies.show()

ratings=movies.select("userID", "movieID", "rating")

#ratings.show()

moviePairs=ratings.alias("ratings1") \
    .join(ratings.alias("ratings2"),(func.col("ratings1.userID")==func.col("ratings2.userID")) & (func.col("ratings1.movieID")<func.col("ratings2.movieID"))) \
    .select(func.col("ratings1.movieID").alias("movie1"), \
            func.col("ratings2.movieID").alias("movie2"), \
            func.col("ratings1.rating").alias("rating1"), \
            func.col("ratings2.rating").alias("rating2"), \
            )
#moviePairs.show()

#print(moviePairs.count())

moviePairsWithNames=moviePairs.join(movieNames.alias("movieNames1"),func.col("movie1")==func.col("movieNames1.movieid")) \
    .join(movieNames.alias("movieNames2"),func.col("movie2")==func.col("movieNames2.movieid")) \
    .select("movie1", "movie2", "rating1", "rating2", func.col("movieNames1.movieName").alias("movie1Name"), func.col("movieNames2.movieName").alias("movie2Name"))

#moviePairsWithNames.show()

#print(moviePairsWithNames.count())

#.cache() is added to store this result in memory
moviePairSimilarities = computeCosineSimilarity(moviePairsWithNames).cache()

#moviePairSimilarities.show()

#movieID 50 is starwars. we are trying to get similar movies to that
movieID=50
scoreThreshold=0.97
coOccurenceThreshold=50

filteredResults=moviePairSimilarities.filter( \
    ((func.col("movie1")==movieID) | (func.col("movie2")==movieID)) & \
    (func.col("score") > scoreThreshold) & \
    (func.col("numPairs") > coOccurenceThreshold) \
    )

#filteredResults.show()

#sort by score desc and take top 10 rows
results=filteredResults.sort(func.col("score").desc()).take(10)

for result in results:
    similarMovieID=result.movie1
    if similarMovieID==movieID:
        print(f"{result.movie2Name} \tscore:{result.score} \tstrength:{result.numPairs}")
    else:
        print(f"{result.movie1Name} \tscore:{result.score} \tstrength:{result.numPairs}")


sparkSessn.stop()
