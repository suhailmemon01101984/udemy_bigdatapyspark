#this script similar to movie-similarities-1m.py. use movie-similarities-1m.py to run local whereas use this file to run in emr cluster
#key changes. updated ratings file to the file containing 1M records. commented section where you are adding jars and aws key and secret
from pyspark import SparkConf, SparkContext
import boto3
from io import StringIO
from math import sqrt
import sys

def loadMovieNames():
    s3=boto3.client('s3')
    bucket_name='suhailmemon84-ml-1m'
    file_key='movies.dat'
    obj=s3.get_object(Bucket=bucket_name, Key=file_key)
    # .decode('ascii', errors='ignore'): Since the file is assumed to be encoded in ASCII, we decode the byte data into a string using the ASCII encoding. The errors='ignore' argument ensures that any characters that cannot be decoded using the ASCII encoding are simply ignored, preventing errors from invalid characters (e.g., non-ASCII characters).
    file_content=obj['Body'].read().decode('ascii', errors='ignore')
    #explanation on why stringio is used is below at the end of this file
    file_like_object = StringIO(file_content)
    movieNames={}
    for line in file_like_object:
        fields=line.split("::")
        movieNames[int(fields[0])]=fields[1]
    return movieNames


def filterDuplicates(joined_Ratings):
    movieIDratings=joined_Ratings[1]
    (movieid1,rating1)=movieIDratings[0]
    (movieid2,rating2)=movieIDratings[1]
    return movieid1<movieid2

def makePairs(unique_Joined_Ratings):
    movieIDratings=unique_Joined_Ratings[1]
    (movieid1,rating1)=movieIDratings[0]
    (movieid2,rating2)=movieIDratings[1]
    return ((movieid1,movieid2),(rating1,rating2))

def computeCosineSimilarity(movie_Pair_Ratings):
    numPairs=0
    sum_xx=0
    sum_yy=0
    sum_xy=0
    for ratingX, ratingY in movie_Pair_Ratings:
        sum_xx=sum_xx+(ratingX*ratingX)
        sum_yy=sum_yy+(ratingY*ratingY)
        sum_xy=sum_xy+(ratingX*ratingY)
        numPairs=numPairs+1
    numerator=sum_xy
    denominator=sqrt(sum_xx)+sqrt(sum_yy)
    score=0
    if(denominator):
        score=numerator/float(denominator)
    return (score, numPairs)


sparkconfig=SparkConf()
#these jars need to be added before you start dealing with aws files. this step only needs to be done if you are trying to run from local machine. if you run this code in EC2 EMR instance like it's primary node, you don't need this step
# sparkconfig = SparkConf() \
#     .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk:1.11.950") \
#     .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
#     .set("spark.hadoop.fs.s3a.access.key", "myawsaccesskeyid") \
#     .set("spark.hadoop.fs.s3a.secret.key", "myawssecretaccesskey")

sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

movieIDmovieNameDict=loadMovieNames()

#change this to ratings.dat when you run on EMR cluster in AWS
data=sc.textFile("s3a://suhailmemon84-ml-1m/ratings.dat")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings=data.map(lambda l:l.split("::")).map(lambda l:(int(l[0]),(int(l[1]),int(l[2]))))

#partition the data by key which in this case is user id. this is done for faster joins
# Self-join to find every combination.
ratingsPartitioned=ratings.partitionBy(100)
joinedRatings=ratingsPartitioned.join(ratingsPartitioned)

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))
#now filter out duplicate pairs.
uniqueJoinedRatings=joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
moviePairsPartitioned=uniqueJoinedRatings.map(makePairs).partitionBy(100)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings=moviePairsPartitioned.groupByKey()
# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities and then sort by key which is (movie1, movie2)
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()
moviePairSimilarities.sortByKey()

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + movieIDmovieNameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(movieIDmovieNameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))

sc.stop()






# explanation of using stringio
# When you read the file from S3, you get the file content as a byte stream. By decoding that byte stream into a string (file_content), you're now working with plain text.
#
# However, many file processing tasks in Python are easier when the file behaves like an actual file (for example, iterating over lines, reading data, etc.). StringIO allows us to convert the string into an in-memory file object, which means you can:
#
# Iterate over it just like you would with a file (using for line in file_like_object).
# Use file handling methods like read(), readline(), or even seek() to manage the "pointer" in the file if needed.
# In this case, the code is leveraging StringIO to allow you to iterate over the file's lines and process them.
