#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from json import loads

import glob

ss = SparkSession.builder.appName("lab6").getOrCreate() # spark session
df = ss.read.format("com.databricks.spark.avro").load("lab5/inputlab5") #load avro files

def getDateHourUrl(file_line):
    datetime = file_line[1]
    datetime_hour = datetime[:10] + ' ' + datetime.split(':')[0][-2:]
    url = file_line[2]
    key = datetime_hour + ':' + url
    return key

# Get the unique url counts
counts_unique_urls = df.rdd\
    .map(getDateHourUrl)\
    .distinct()\
    .map(lambda key: (key.split(":")[0], 1))\
    .reduceByKey(lambda a, b: a + b)

#defining schema for DF
schema = StructType([StructField("datetime_hour", StringType(),
True),StructField("count_unique_url", IntegerType(), True)])
counts_unique_urls = ss.createDataFrame(counts_unique_urls, schema) # create Dataframe
counts_unique_urls.show()

#define catalog for hbase table - maps the schema from Apache Spark to Apache HBase.
catalog = ''.join("""{
"table":{"namespace":"lab6", "name":"date_hour"},
"rowkey":"key",
"columns":{
"datetime_hour":{"cf":"rowkey", "col":"key", "type":"string"},
"count_unique_url":{"cf":"data", "col":"count_unique_url", "type":"int"}
}
}""".split())

#write to hbase
counts_unique_urls.write \
.options(catalog=catalog,newtable=5) \
.format('org.apache.spark.sql.execution.datasources.hbase') \
.mode("overwrite") \
.save()

ss.stop()
