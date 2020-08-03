from __future__ import print_function
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    """
        Spark
    """
    spark = SparkSession\
        .builder\
        .appName("QueryFour")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    #----------
    #Query Four

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: reading from local filesystem")
        df = spark.read.load("hw5_problem2.parquet")
    else:
        print("WARNING: reading from S3")
        df = spark.read.load("s3n://csci.e-88.principles.of.big.data.processing/hw5_problem2.parquet")

    print('== read parquet ==')
    df.explain()    #explain

    # select datetime, country, urls columns
    dfReduced = df.select(from_unixtime(unix_timestamp('_c1', "yyyy-MM-dd'T'HH:mm:ss")).alias('datetime')\
                            ,col('_c4').alias('country')\
                            ,col('_c2').alias('url')\
                            )
    
    print('== select datetime, country, urls columns ==')
    dfReduced.explain()     #explain

    # specify datetime range
    t1 = '2019-09-13 17:00:00'
    t2 = '2019-09-14 09:00:00'

    # filter by datetime range, add datehour column, remove duplicate rows
    dfReduced = dfReduced\
        .filter(dfReduced["datetime"] >= lit(t1))\
        .filter(dfReduced["datetime"] <= lit(t2))\
        .withColumn(  'datehour' , date_format(col("datetime"), "yyyy-MM-dd:HH") )\
        .select('datehour','country','url').drop_duplicates()

    print('== filter by datetime range, add datehour column, remove duplicate rows ==')
    dfReduced.explain()     #explain
 
    queryOut = dfReduced.groupBy("datehour", "country").agg(count("*")).orderBy('datehour','country', ascending=True)
    
    print('== get count of unique urls by country by hour ==')
    queryOut.explain()      #explain   

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: writing to local filesystem")
        queryOut.coalesce(1).write.csv('hw5_problem3_q4.csv')

    else:
        print("WARNING: writing to S3")
        queryOut.coalesce(1).write.csv("s3a://csci.e-88.principles.of.big.data.processing/spark_queries/hw5_problem3_q4_emr.csv")

    print("DONE")

    #-----------
    
    spark.stop()
