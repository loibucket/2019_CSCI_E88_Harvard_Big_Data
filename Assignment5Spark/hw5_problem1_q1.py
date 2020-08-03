from __future__ import print_function
import sys

from pyspark.sql import SparkSession

#get all the dateHour and url pairs
def getDateHourUrl(line):
    columns = line.split(",")
    dateHour = columns[1].split(":")[0]
    url = columns[2]
    return ( dateHour, url )

if __name__ == "__main__":
    """
        Spark
    """
    spark = SparkSession\
        .builder\
        .appName("QueryOne")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    #----------
    #Query One

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: reading from local filesystem")
        logs = spark.sparkContext.textFile("input_files/*.csv")
    else:
        print("WARNING: reading from S3")
        logs = spark.sparkContext.textFile("s3://csci.e-88.principles.of.big.data.processing/input_files/*.csv")

    #map, group, sort and collect results
    collection = logs.map(getDateHourUrl).distinct()
    queryOut = collection.map(lambda x: (x[0], x[1]))\
        .groupByKey()\
        .mapValues(lambda vals: len(set(vals)))\
        .sortByKey()\

    if len(sys.argv) > 1:
        queryOut.saveAsTextFile("spark_q1")
    else:
        queryOut.saveAsTextFile("s3://csci.e-88.principles.of.big.data.processing/spark_queries/spark_emr_q1")


    print("DONE")

    #-----------
    
    spark.stop()