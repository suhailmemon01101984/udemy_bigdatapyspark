from pyspark import SparkConf, SparkContext

sparkconfig=SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")


def parseLine(line):
    fields=line.split(",")
    customerID=fields[0]
    spendAmount=float(fields[2])
    return customerID,spendAmount

lines=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/customer-orders.csv")
customerIDspendAmout=lines.map(parseLine)
totalSpendAmountByCustomer=customerIDspendAmout.reduceByKey(lambda x,y:x+y).sortBy(lambda x: x[1], ascending=False)

results=totalSpendAmountByCustomer.collect()
for result in results:
    print(f'{result[0]}\t{round(result[1],2)}')


quit()
