from __future__ import print_function
import sys

from pyspark.sql import SparkSession

#get all the dateHourUrl and user pairs
def getDateHourUrlUser(line):
    columns = line.split(",")
    dateHour = columns[1].split(":")[0]
    url = columns[2]
    user = columns[3]
    return ( "::".join([dateHour, url]), user )

if __name__ == "__main__":
    """
        Spark
    """
    spark = SparkSession\
        .builder\
        .appName("QueryThree")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    #----------
    #Query Three

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: reading from local filesystem")
        logs = spark.sparkContext.textFile("input_files/*.csv")
    else:
        print("WARNING: reading from S3")
        logs = spark.sparkContext.textFile("s3://csci.e-88.principles.of.big.data.processing/input_files/*.csv")

    #map, group, sort and collect results
    #nearly identical to query two, but mapValue uses a list instead of set to keep duplicates
    collection = logs.map(getDateHourUrlUser) 
    queryOut = collection.map(lambda x: (x[0], x[1]))\
        .groupByKey()\
        .mapValues(lambda vals: len(list(vals)))\
        .sortByKey()\

    if len(sys.argv) > 1:
        queryOut.saveAsTextFile("spark_q3")
    else:
        queryOut.saveAsTextFile("s3://csci.e-88.principles.of.big.data.processing/spark_queries/spark_emr_q3")


    print("DONE")

    #-----------
    
    spark.stop()