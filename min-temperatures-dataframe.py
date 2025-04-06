#problem statement: take input text file: 1800.csv which contains info on stations and temps and get minimum temperature for each stationid in Farenheit

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

sparkSessn=SparkSession.builder.appName("MinTemperatures").getOrCreate()

#true means column allows nulls
temperatures_schema=StructType([StructField("stationID", StringType(), True), StructField("date", IntegerType(), True), StructField("measure_type", StringType(), True), StructField("temperature", FloatType(), True)])

df=sparkSessn.read.schema(temperatures_schema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/1800.csv")

df.printSchema()

onlymintemps=df.filter(df.measure_type=="TMIN")

station_n_temps=onlymintemps.select("stationID","temperature")

minTempsByStation=station_n_temps.groupBy("stationID").min("temperature") # if you want to alias the column run it this way: station_n_temps.groupBy("stationID").agg(func.min("temperature").alias("minTemp"))

minTempsByStationF=minTempsByStation.withColumn("Farenheit_temp", func.round(func.col("min(temperature)")*0.1*(9.0/5.0)+32, 2)).select("stationID", "Farenheit_temp").sort("Farenheit_temp")

minTempsByStationF.printSchema()

finalresults=minTempsByStationF.collect()

for result in finalresults:
    print(f"{result[0]}    {result[1]}F")

sparkSessn.stop()
