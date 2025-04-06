from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

sparkSessn=SparkSession.builder.appName("MinTemps").getOrCreate()

temperaturesSchema=StructType([
 StructField("stationID", StringType(), True), \
    StructField("date", IntegerType(), True), \
    StructField("measureType", StringType(), True), \
    StructField("temperature", FloatType(), True) \
])

inputDF=sparkSessn.read.schema(temperaturesSchema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/1800.csv")
inputDF.printSchema()
inputDF.show()

minTempsDF=inputDF.filter(inputDF.measureType=="TMIN")
stationTempsDF=minTempsDF.select("stationID","temperature")
minTempsByStationDF=stationTempsDF.groupBy("stationID").agg(func.min("temperature").alias("minTemp"))
minTempsByStationFarenheitDF=minTempsByStationDF.withColumn("MinFarenheitTemp",func.round(func.col("minTemp")*0.1*(9.0/5.0)+32,2)).select("stationID","MinFarenheitTemp").sort("MinFarenheitTemp",ascending=True)
minTempsByStationFarenheitDF.show()
minTempsByStationFarenheitDF.printSchema()

finalResults=minTempsByStationFarenheitDF.collect()
print(type(finalResults))

for result in finalResults:
    print(f"{result[0]}    {result[1]}F")

sparkSessn.stop()
