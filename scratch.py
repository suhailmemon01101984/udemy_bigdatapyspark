from pyspark import SparkConf, SparkContext

sparkconfig=SparkConf().setMaster("local").setAppName("FriendsByAge")
sc=SparkContext(conf=sparkconfig)


def parseline(line):
    fields=line.split(',')
    age=int(fields[2])
    num_friends=int(fields[3])
    return age,num_friends

lines=sc.textFile("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/fakefriends.csv")
age_n_friends=lines.map(parseline)
age_sum_friends_n_count=age_n_friends.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
avg_age_by_friends=age_sum_friends_n_count.mapValues(lambda x:x[0]/x[1])
results=avg_age_by_friends.collect()
for row in results:
    print(row)

quit()
