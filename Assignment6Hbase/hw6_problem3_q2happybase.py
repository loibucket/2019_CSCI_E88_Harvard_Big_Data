from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

import happybase

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

    #map, group, sort and collect results
    #mapValues with set is used to remove duplicates
    collection = logs.map(getDateHourUrlUser)
    queryTwo = collection.map(lambda x: (x[0], x[1]))\
        .groupByKey()\
        .mapValues(lambda vals: len(set(vals)))\
        .sortByKey()\

    #put data in hbase
    connection = happybase.Connection(host='localhost', port=9090)

    connection.create_table( 'query2', {'count': dict()} )

    queryTwoTable = connection.table('query2')
    
    for f in queryTwo.collect():
        queryTwoTable.put(f[0],{b'count:count':str(f[1])})

    print("DONE")

    #-----------
    
    spark.stop()