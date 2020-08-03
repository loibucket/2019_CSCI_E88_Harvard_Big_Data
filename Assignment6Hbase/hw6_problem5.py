from pyspark import SQLContext
from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .appName("problem5") \
     .getOrCreate()

sc = spark.sparkContext
sqlc = SQLContext(sc)

sc.setLogLevel("WARN")

#setup what to read
catalog = ''.join("""{
"table":{"namespace":"default", "name":"query2"},
"rowkey":"key",
"columns":{
"date_hour_url":{"cf":"rowkey", "col":"key", "type":"string"},
"count":{"cf":"count", "col":"count", "type":"int"}
}
}""".split())

#read from hbase
data_source_format = 'org.apache.spark.sql.execution.datasources.hbase'

df = sqlc.read\
.options(catalog=catalog,newtable=5)\
.format(data_source_format)\
.load()

# keys
# '2019-09-12:02:http://example.com/?url=003'

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("allData")
# Use SQL commands to get url events for specified hour
selectedHourUrlDF = spark.sql("""
    SELECT date_hour_url, count FROM allData
    WHERE date_hour_url = '2019-09-12:02:http://example.com/?url=003'
    """)

print("== explain query execution plan ==")
selectedHourUrlDF.explain()    #explain

selectedHourUrlDF.show()

print("DONE")

spark.stop()