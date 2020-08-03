from __future__ import print_function
import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Spark
    """
    spark = SparkSession\
        .builder\
        .appName("QueryThreeSQL")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    #----------
    #Query3: get count of events per URL for a specified hour: 2019-09-12:14

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: reading from local filesystem")
        df = spark.read.load("hw5_problem2.parquet")
    else:
        print("WARNING: reading from S3")
        df = spark.read.load("s3a://csci.e-88.principles.of.big.data.processing/hw5_problem2.parquet")

    print('== read parquet ==')
    df.explain()    #explain

    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("allData")
    # Use SQL commands to get url events for specified hour
    selectedHourDF = spark.sql("""
        SELECT _c1, _c2 FROM allData
        WHERE _c1 LIKE '%2019-09-12T14%'
        """)

    print('== select datetime, url columns and filter by specified hour ==')
    selectedHourDF.explain()    #explain

    # Register the DataFrame as a SQL temporary view
    selectedHourDF.createOrReplaceTempView("selectedHourData")
    # Use SQL commands to get event counts for each url
    queryOut = spark.sql("""
        SELECT _c2, COUNT(_c2) 
        FROM selectedHourData
        GROUP BY _c2
        """).orderBy('_c2', ascending=True)

    print('== get event counts for each url ==')
    queryOut.explain()    #explain

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: writing to local filesystem")
        queryOut.coalesce(1).write.csv('hw5_problem4_q3sql.csv')

    else:
        print("WARNING: writing to S3")
        queryOut.coalesce(1).write.csv("s3a://csci.e-88.principles.of.big.data.processing/spark_queries/hw5_problem4_q3sql_emr.csv")

    print("DONE")

    #-----------
    
    spark.stop()
