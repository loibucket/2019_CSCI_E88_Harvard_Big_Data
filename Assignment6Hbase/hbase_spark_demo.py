from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from json import loads

spark = SparkSession\
    .builder\
    .appName("Demo")\
    .getOrCreate()

df = spark.sparkContext.parallelize([('a', 'def'), ('b', 'abc')]).toDF(schema=['col0', 'col1'])

# ''.join(string.split()) in order to write a multi-line JSON string here.
catalog = ''.join("""{
    "table":{"namespace":"default", "name":"testtable"},
    "rowkey":"key",
    "columns":{
        "col0":{"cf":"rowkey", "col":"key", "type":"string"},
        "col1":{"cf":"cf", "col":"col1", "type":"string"}
    }
}""".split())

df.show()

#data_source_format = 'org.apache.hadoop.hbase.spark'
data_source_format = 'org.apache.spark.sql.execution.datasources.hbase'

df.write\
    .options(catalog=catalog,newtable=5)\
    .format(data_source_format)\
    .save()

spark.stop()