from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

sparkSessn=SparkSession.builder.appName("MinTemperatures").getOrCreate()

#true means column allows nulls
temperatures_schema=StructType[StructField("stationID", StringType, True), StructField("date", IntegerType, True), StructField("measure_type", StringType, True), StructField("temperature", FloatType, True)]

df=sparkSessn.read.schema(temperatures_schema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/1800.csv")

df.printSchema()


