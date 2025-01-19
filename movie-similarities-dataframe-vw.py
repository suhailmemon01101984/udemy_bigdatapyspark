# this is a full redo of movie-similarities-dataframe.py but through using pyspark sql views. run below code with parameter as 50 which is the movieID for starwars. right click on file -> modify run configuration and put 50 under parameters and click OK
#comment here
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
import sys

# def computeCosineSimilarity(data):
#     # Compute xx, xy and yy columns
#     pairScores = data \
#       .withColumn("xx", func.col("rating1") * func.col("rating1")) \
#       .withColumn("yy", func.col("rating2") * func.col("rating2")) \
#       .withColumn("xy", func.col("rating1") * func.col("rating2"))
#
#     # Compute numerator, denominator and numPairs columns
#     calculateSimilarity = pairScores \
#       .groupBy("movie1", "movie2", "movie1Name", "movie2Name") \
#       .agg( \
#         func.sum(func.col("xy")).alias("numerator"), \
#         (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
#         func.count(func.col("xy")).alias("numPairs")
#       )
#
#     # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
#     result = calculateSimilarity \
#       .withColumn("score", \
#         func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
#           .otherwise(0) \
#       ).select("movie1", "movie2", "movie1Name", "movie2Name", "score", "numPairs")
#
#     return result

sparkSessn = SparkSession.builder.appName("MovieSimilarities").getOrCreate()

movieNameSchema = StructType([StructField("movieid", IntegerType(), True), \
                              StructField("movieName", StringType(), True)])

movieSchema = StructType([StructField("userID", IntegerType(), True), \
                          StructField("movieID", IntegerType(), True), \
                          StructField("rating", IntegerType(), True), \
                          StructField("timestamp", LongType(), True)])

movieNames = sparkSessn.read.option("sep", "|").schema(movieNameSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/ml-100k/u.item")
movies = sparkSessn.read.option("sep", "\t").schema(movieSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/ml-100k/u.data")

movieNames.createOrReplaceTempView("movieNames_vw")
movies.createOrReplaceTempView("movies_vw")

ratings = sparkSessn.sql("select userID, movieID, rating from movies_vw")
ratings.createOrReplaceTempView("ratings_vw")

moviePairs = sparkSessn.sql("select r1.movieID as movieID1, r2.movieID as movieID2, r1.rating as rating1, r2.rating as rating2 from ratings_vw as r1 join ratings_vw as r2 on r1.userID=r2.userID and r1.movieID<r2.movieID")
moviePairs.createOrReplaceTempView("moviePairs_vw")

moviePairsWithNames = sparkSessn.sql("select mpv.movieID1, mpv.movieID2, mpv.rating1, mpv.rating2, mn1.movieName as movieName1, mn2.movieName as movieName2 from moviePairs_vw mpv join movieNames_vw mn1 on mpv.movieID1=mn1.movieID join movieNames_vw mn2 on mpv.movieID2=mn2.movieID")
moviePairsWithNames.createOrReplaceTempView("moviePairsWithNames_vw")

query = """
    WITH rs1 AS (
        SELECT
            movieID1,
            movieID2,
            movieName1,
            movieName2,
            sum(rating1*rating1) as sum_xx,
            sum(rating1*rating2) as sum_xy,
            sum(rating2*rating2) as sum_yy,
            count(*) as numPairs
        FROM moviePairsWithNames_vw
        group by 1,2,3,4
    )
    SELECT
        movieID1,
        movieID2,
        movieName1,
        movieName2,
        case when sum_xx * sum_yy = 0 then 0 else sum_xy/(sqrt(sum_xx) * sqrt(sum_yy)) end as score,
        numPairs
    FROM rs1
"""

# Execute the SQL query
moviePairSimilarities = sparkSessn.sql(query).cache()
moviePairSimilarities.createOrReplaceTempView("moviePairSimilarities_vw")

# ratings=movies.select("userID", "movieID", "rating")


# moviePairs=ratings.alias("ratings1") \
#     .join(ratings.alias("ratings2"),(func.col("ratings1.userID")==func.col("ratings2.userID")) & (func.col("ratings1.movieID")<func.col("ratings2.movieID"))) \
#     .select(func.col("ratings1.movieID").alias("movie1"), \
#             func.col("ratings2.movieID").alias("movie2"), \
#             func.col("ratings1.rating").alias("rating1"), \
#             func.col("ratings2.rating").alias("rating2"), \
#             )

# moviePairsWithNames=moviePairs.join(movieNames.alias("movieNames1"),func.col("movie1")==func.col("movieNames1.movieid")) \
#     .join(movieNames.alias("movieNames2"),func.col("movie2")==func.col("movieNames2.movieid")) \
#     .select("movie1", "movie2", "rating1", "rating2", func.col("movieNames1.movieName").alias("movie1Name"), func.col("movieNames2.movieName").alias("movie2Name"))


# .cache() is added to store this result in memory
# moviePairSimilarities = computeCosineSimilarity(moviePairsWithNames).cache()


if (len(sys.argv) > 1):
    # movieID 50 is starwars. we are trying to get similar movies to that
    # movieID=50
    inputmovieID = int(sys.argv[1])
    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    # filteredResults=moviePairSimilarities.filter( \
    #     ((func.col("movieID1") == inputmovieID) | (func.col("movieID2") == inputmovieID)) & \
    #     (func.col("score") > scoreThreshold) & \
    #     (func.col("numPairs") > coOccurenceThreshold) \
    #     )
    #
    # #sort by score desc and take top 10 rows
    # results=filteredResults.sort(func.col("score").desc()).take(10)

    filteredResults = sparkSessn.sql(f"select movieID1, movieID2, movieName1, movieName2, score, numPairs from moviePairSimilarities_vw where score > {scoreThreshold} and numPairs > {coOccurenceThreshold} and (movieID1={inputmovieID} or movieID2={inputmovieID}) order by score desc limit 10")


    for result in filteredResults.collect():
        similarMovieID = result.movieID1
        if similarMovieID == inputmovieID:
            print(f"{result.movieName2} \tscore:{result.score} \tstrength:{result.numPairs}")
        else:
            print(f"{result.movieName1} \tscore:{result.score} \tstrength:{result.numPairs}")

sparkSessn.stop()
