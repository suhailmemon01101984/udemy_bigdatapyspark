from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
import sys

sparkSessn=SparkSession.builder.appName("GetSimilarMovies").getOrCreate()

movieNameSchema=StructType([
    StructField("movieID", IntegerType(), True), \
    StructField("movieName", StringType(), True)
])

movieSchema=StructType([
    StructField("userID", IntegerType(), True), \
    StructField("movieID", IntegerType(), True), \
    StructField("rating", IntegerType(), True), \
    StructField("timestamp", LongType(), True)
])

movieNamesDF=sparkSessn.read.option("sep", "|").schema(movieNameSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/ml-100k/u.item")
moviesDF=sparkSessn.read.option("sep", "\t").schema(movieSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/ml-100k/u.data")

#movieNamesDF.show()
#moviesDF.show()

movieNamesDF.createOrReplaceTempView("movieNames_vw")
moviesDF.createOrReplaceTempView("movies_vw")

ratingsDF=sparkSessn.sql("select userID, movieID, rating from movies_vw")

#ratingsDF.show()

ratingsDF.createOrReplaceTempView("ratings_vw")

moviePairsDF=sparkSessn.sql("select r1.movieID as movieID1, r2.movieID as movieID2, r1.rating as rating1, r2.rating as rating2 from ratings_vw as r1 join ratings_vw as r2 on r1.userID=r2.userID and r1.movieID<r2.movieID")

#moviePairsDF.show()

moviePairsDF.createOrReplaceTempView("moviePairs_vw")

moviePairsWithNamesDF=sparkSessn.sql("select mp.movieID1, mp.movieID2, mp.rating1, mp.rating2, mn1.movieName as movieName1, mn2.movieName as movieName2 from moviePairs_vw mp join movieNames_vw mn1 on mp.movieID1=mn1.movieID join movieNames_vw mn2 on mp.movieID2=mn2.movieID")

#moviePairsWithNamesDF.show()

moviePairsWithNamesDF.createOrReplaceTempView("moviePairsWithNames_vw")

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

moviePairSimilaritiesDF=sparkSessn.sql(query)
#moviePairSimilaritiesDF.show()
moviePairSimilaritiesDF.createOrReplaceTempView("moviePairSimilarities_vw")

if (len(sys.argv)>1):
    inputMovieID=int(sys.argv[1])
    scoreThreshold=0.97
    coOccurrenceThreshold=50
    filteredResultsDF=sparkSessn.sql(f"select movieID1, movieID2, movieName1, movieName2, score, numPairs from moviePairSimilarities_vw where score > {scoreThreshold} and numPairs > {coOccurrenceThreshold} and (movieID1={inputMovieID} or movieID2={inputMovieID}) order by score desc limit 10")
    filteredResultsDF.show()

    for result in filteredResultsDF.collect():
        similarMovieID=result.movieID1
        if similarMovieID==inputMovieID:
            print(f"{result.movieName2}\tscore:{result.score}\tstrength:{result.numPairs}")
        else:
            print(f"{result.movieName1}\tscore:{result.score}\tstrength:{result.numPairs}")

sparkSessn.stop()
