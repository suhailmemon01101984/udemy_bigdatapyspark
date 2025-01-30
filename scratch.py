from pyspark import SparkConf, SparkContext

sparkconfig=SparkConf().setMaster("local").setAppName("MaxTemps")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

def parseline(line):
    fields=line.split(',')
    stationID=fields[0]
    entryType=fields[2]
    tempF=(float(fields[3])*0.1*(9.0/5.0))+32
    return stationID, entryType, tempF


lines=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/1800.csv")
station_temptype_temps=lines.map(parseline)
only_max_temps=station_temptype_temps.filter(lambda x: 'TMAX' in x[1])
station_temps=only_max_temps.map(lambda x: (x[0],x[2]))
max_temp_by_station=station_temps.reduceByKey(lambda x,y: max(x,y))

results=max_temp_by_station.collect()
for result in results:
    print(f'{result[0]}\t{round(result[1],2)}F')

quit()
