from pyspark import SparkConf, SparkContext

sparkconfig=SparkConf().setMaster("local").setAppName("MinTemps")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")


def parseline(line):
    fields=line.split(',')
    stationID=fields[0]
    entryType=fields[2]
    tempf=(float(fields[3])*0.1*(9.0/5.0))+32.0
    return stationID,entryType,tempf

lines=sc.textFile('/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/1800.csv')
station_type_temps=lines.map(parseline)
min_station_type_temps=station_type_temps.filter(lambda x: "TMAX" in x[1])
station_n_temps=min_station_type_temps.map(lambda x: (x[0],x[2]))
min_temp_by_station=station_n_temps.reduceByKey(lambda x,y:max(x,y))

results=min_temp_by_station.collect()
for row in results:
    print(f'{row[0]}    {round(row[1],2)}F')

quit()
