###run this code and copy the access_log.txt into the /Users/suhailmemon/Downloads/logs directory and then do another file and do another file and see how the status counts change

from pyspark.sql import SparkSession

import pyspark.sql.functions as func

# Create a SparkSession (the config bit is only for Windows!)
sparkconfig = SparkSession.builder.appName("TopUrls").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = sparkconfig.readStream.text("/Users/suhailmemon/Downloads/logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(func.regexp_extract('value', hostExp, 1).alias('host'),
                         func.regexp_extract('value', timeExp, 1).alias('timestamp'),
                         func.regexp_extract('value', generalExp, 1).alias('method'),
                         func.regexp_extract('value', generalExp, 2).alias('endpoint'),
                         func.regexp_extract('value', generalExp, 3).alias('protocol'),
                         func.regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         func.regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF2= logsDF.withColumn("eventTime", func.current_timestamp())

endpointCounts=logsDF2.groupBy(func.window(func.col("eventTime"),windowDuration="30 seconds", slideDuration="10 seconds"), func.col("endpoint")).count()

sortedEndpointCounts=endpointCounts.orderBy(func.col("count").desc())
# Give the query a name: counts, Kick off our streaming query with the .start() call , dumping complete results to the console using outputMode("complete").format("console")
query = ( sortedEndpointCounts.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
sparkconfig.stop()

