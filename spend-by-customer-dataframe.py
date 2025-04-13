#problem statement: take input text file: customer-orders.csv which contains info like customer id, item id and amount spent on item. get total amount spent by customer sorted by total amount spent
#dataset containing customer id, item id and how much customer spent on that item. how much in total customer spent

#"/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/customer-orders.csv"

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

sparkSessn=SparkSession.builder.appName("spendByCustomer").getOrCreate()

customer_orders_schema=StructType([StructField("customerID", IntegerType(), True), StructField("itemID", IntegerType(), True), StructField("amount_spent", FloatType(), True)])

customers_orders_df=sparkSessn.read.schema(customer_orders_schema).csv("/Users/suhailmemon/Documents/MACBOOKPRO/dell laptop/Desktop/git/udemy_bigdatapyspark/datafiles/customer-orders.csv")

customers_orders_df.printSchema()

customer_spend_df=customers_orders_df.select("customerID","amount_spent")

total_spend_by_customer_df=customer_spend_df.groupBy("customerID").sum("amount_spent")

#one way to do this
totalByCustomerSorted = customers_orders_df.groupBy("customerID").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent")).sort("total_spent")

totalByCustomerSorted.printSchema()

#another way to do this using withcolumn
total_spend_by_customer_df2=total_spend_by_customer_df.withColumn("totalAmountSpent", func.round("sum(amount_spent)",2)).select("customerID", "totalAmountSpent").sort("totalAmountSpent")

total_spend_by_customer_df2.printSchema()

totalByCustomerSorted.show(totalByCustomerSorted.count())
total_spend_by_customer_df2.show(total_spend_by_customer_df2.count())

sparkSessn.stop()
