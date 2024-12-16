from pyspark import SparkConf, SparkContext
import math

sparkconfig = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

def parseLine(line):
    fields = line.split(',')
    stationid = fields[0]
    entrytype = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return stationid, entrytype, temperature


lines = sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/1800.csv")
parsedlines = lines.map(parseLine)
mintemps=parsedlines.filter(lambda x: "TMIN" in x[1])
station_n_temps=mintemps.map(lambda x: (x[0],x[2]))
min_temps_by_stations=station_n_temps.reduceByKey(lambda x,y: min(x,y))
results=min_temps_by_stations.collect()
for eachrow in results:
    print(f'{eachrow[0]}    {round(eachrow[1],2)}F')
quit()
