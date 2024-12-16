from pyspark import SparkConf, SparkContext
import math

sparkconfig=SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc=SparkContext(conf=sparkconfig)
sc.setLogLevel("ERROR")

def parseline(line):
    fields=line.split(",")
    customerid=int(fields[0])
    spendamount=float(fields[2])
    return customerid, spendamount

lines=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/customer-orders.csv")
customerid_n_spendamounts=lines.map(parseline)
total_spend_by_customer_sorted=customerid_n_spendamounts.reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1]) # this command will total the spend by customer and then sort the results by the second column which is the spend.
results=total_spend_by_customer_sorted.collect()
for eachrow in results:
    print(f'{eachrow[0]},{round(eachrow[1],2)}')

quit()
