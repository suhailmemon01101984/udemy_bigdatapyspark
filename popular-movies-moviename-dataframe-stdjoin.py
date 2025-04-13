#problem statement: take input text file: u.data which contains userid, movieid, rating and timestamp and file u.item which contains movie titles among other things  output most popular movie names and their IDs by rating count

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

sparkSessn=SparkSession.builder.appName("PopularMovies").getOrCreate()

movieSchema= StructType([StructField("userID", IntegerType(), True), \
                         StructField("movieID", IntegerType(), True), \
                         StructField("rating", IntegerType(), True), \
                         StructField("timestamp", IntegerType(), True)])

movienameSchema= StructType([StructField("movieID", IntegerType(), True), \
                             StructField("movieTitle", StringType(), True), \
                             StructField("releaseDate", StringType(), True), \
                             StructField("videoReleaseDate", StringType(), True), \
                             StructField("imdbURL", StringType(), True), \
                             StructField("unknown", IntegerType(), True), \
                             StructField("action", IntegerType(), True), \
                             StructField("adventure", IntegerType(), True), \
                             StructField("animation", IntegerType(), True), \
                             StructField("children", IntegerType(), True), \
                             StructField("comedy", IntegerType(), True), \
                             StructField("crime", IntegerType(), True), \
                             StructField("documentary", IntegerType(), True), \
                             StructField("drama", IntegerType(), True), \
                             StructField("fantasy", IntegerType(), True), \
                             StructField("filmNoir", IntegerType(), True), \
                             StructField("horror", IntegerType(), True), \
                             StructField("musical", IntegerType(), True), \
                             StructField("mystery", IntegerType(), True), \
                             StructField("romance", IntegerType(), True), \
                             StructField("scifi", IntegerType(), True), \
                             StructField("thriller", IntegerType(), True), \
                             StructField("war", IntegerType(), True), \
                             StructField("western", IntegerType(), True)])

moviesDF=sparkSessn.read.option("sep", "\t").schema(movieSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/ml-100k/u.data")
movienameDF=sparkSessn.read.option("sep", "|").schema(movienameSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/ml-100k/u.item")

moviesDF.printSchema()
movienameDF.printSchema()

moviesDF.createOrReplaceTempView("moviesDF_vw")
movienameDF.createOrReplaceTempView("movienameDF_vw")

movieCountsWithMovieNames=sparkSessn.sql("select m.movieID, mn.movieTitle, count(*) as ratings_count from moviesDF_vw m join movienameDF_vw mn on m.movieID=mn.movieID group by 1,2 order by 3 desc")

print(type(movieCountsWithMovieNames))

movieCountsWithMovieNames.show(10,False) # False means do not truncate the columns (i.e., show the full content of each column, even if it's long)

#another way to collect. the [:10] is added for the top 10 rows
print("|movieID|movieTitle|ratings_count|")
for eachrow in movieCountsWithMovieNames.collect()[:10]:
    print(f"|{eachrow[0]}|{eachrow[1]}|{eachrow[2]}|")

sparkSessn.stop()




# random example when you need to run some more complex sqls using with clauses and stuff

# Perform a complex SQL query
# query = """
#     WITH MovieAvgRatings AS (
#         SELECT
#             m.movieID,
#             m.title,
#             m.genre,
#             AVG(r.rating) AS avg_rating,
#             COUNT(r.rating) AS rating_count
#         FROM movies m
#         JOIN ratings r ON m.movieID = r.movieID
#         GROUP BY m.movieID, m.title, m.genre
#     ),
#     RankedMovies AS (
#         SELECT
#             movieID,
#             title,
#             genre,
#             avg_rating,
#             rating_count,
#             RANK() OVER (PARTITION BY genre ORDER BY avg_rating DESC) AS rank
#         FROM MovieAvgRatings
#         WHERE rating_count >= 100
#     )
#     SELECT
#         genre,
#         title,
#         avg_rating,
#         rating_count,
#         rank
#     FROM RankedMovies
#     WHERE rank <= 5
#     ORDER BY genre, rank
# """
#
# # Execute the SQL query
# top_movies = spark.sql(query)
#
# # Show the results
# top_movies.show(20, False)
