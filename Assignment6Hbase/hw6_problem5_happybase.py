import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import happybase

#create spark session
spark = SparkSession.builder \
     .appName("problem5") \
     .getOrCreate()

spark = SparkSession.builder.appName("lab6").getOrCreate() # spark session

#define schema
schema = StructType([StructField("date_hour_url", StringType(), True),StructField("count_unique_users", IntegerType(), True)])

#make empty dataframe
df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

#list of keys to retrieve
listOfKeys = ['2019-09-12:02:http://example.com/?url=003',\
              '2019-09-12:02:http://example.com/?url=004',\
              '2019-09-12:02:http://example.com/?url=005',\
              '2019-09-12:02:http://example.com/?url=006']

#retreive keys into dataframe
for key in listOfKeys:
    connection = happybase.Connection(host='localhost', port=9090)
    table = happybase.Table(b'query2',connection)
    newRow = spark.createDataFrame([(   key   ,   int(table.row(key)[b'count:count']))])
    df = df.union(newRow)

#view data using sparkSQL

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("allData")

# Use SQL commands to get url events for specified hour
sqlDF = spark.sql("""
    SELECT * FROM allData
    """)

print("== explain query execution plan ==")
sqlDF.explain()    #explain

sqlDF.show()

print("DONE")

spark.stop()