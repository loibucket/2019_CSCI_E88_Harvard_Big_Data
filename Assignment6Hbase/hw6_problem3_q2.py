from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from json import loads

#get all the dateHourUrl and user pairs
def getDateHourUrlUser(line):
    columns = line.split(",")
    dateHour = columns[1].split(":")[0]
    dateHour = dateHour.replace('T', ':', 1)
    url = columns[2]
    user = columns[3]
    return ( ":".join([dateHour, url]), user )

if __name__ == "__main__":
    """
        Spark
    """
    spark = SparkSession\
        .builder\
        .appName("QueryTwo")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    #----------
    #Query Two

    logs = spark.sparkContext.textFile("s3://csci.e-88.principles.of.big.data.processing/input_files/*.csv")
    #logs = spark.sparkContext.textFile("input_files/*.csv")

    #map, group, sort and collect results
    #mapValues with set is used to remove duplicates
    collection = logs.map(getDateHourUrlUser)
    queryTwo = collection.map(lambda x: (x[0], x[1]))\
        .groupByKey()\
        .mapValues(lambda vals: len(set(vals)))\
        .sortByKey()\

    #defining schema for DF
    schema = StructType([\
                        StructField("date_hour_url", StringType(),True),\
                        StructField("unique_user_count", IntegerType(), True)\
                        ])

    #create Dataframe
    queryTwo = spark.createDataFrame(queryTwo, schema) 
    queryTwo.show(20)

    #define catalog for hbase table - maps the schema from Apache Spark to Apache HBase.
    catalog = ''.join("""{
        "table":{"namespace":"problem3", "name":"query2"},
        "rowkey":"key",
        "columns":{
            "date_hour_url":{"cf":"rowkey", "col":"key", "type":"string"},
            "unique_user_count":{"cf":"data", "col":"unique_user_count", "type":"int"}
            }
    }""".split())

    #write to hbase
    queryTwo.write\
        .options(catalog=catalog,newtable=5)\
        .format('org.apache.spark.sql.execution.datasources.hbase')\
        .save()

    print("DONE")

    #-----------
    
    spark.stop()