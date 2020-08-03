from __future__ import print_function
import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Spark
    """
    spark = SparkSession\
        .builder\
        .appName("QueryFive")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    #----------
    #Query5

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: reading from local filesystem")
        df = spark.read.load("hw5_problem2.parquet")
    else:
        print("WARNING: reading from S3")
        df = spark.read.load("s3a://csci.e-88.principles.of.big.data.processing/hw5_problem2.parquet")

    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("allData")
    # Downselect data
    avgTtfbDF = spark.sql("""
        WITH cte AS
            ( SELECT SUBSTRING(_c1,1,10) AS day, _c2 AS url, _c8 AS ttfb 
            FROM allData )
        SELECT day,url,AVG(ttfb) as avg_ttfb FROM cte
        GROUP BY day,url
        ORDER BY day,url,avg_ttfb
        """)

    # Register the DataFrame as a SQL temporary view
    avgTtfbDF.createOrReplaceTempView("avgData")
    # Downselect data
    queryOut = spark.sql("""
        WITH cte AS
            ( SELECT day, url, avg_ttfb, ROW_NUMBER() OVER (PARTITION BY day ORDER BY avg_ttfb ASC) AS ttfb_rank
            FROM avgData )
        SELECT day,url,avg_ttfb FROM cte
        WHERE ttfb_rank <= 5
        ORDER BY day,avg_ttfb
        """)

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: writing to local filesystem")
        queryOut.coalesce(1).write.csv('hw5_problem5.csv')

    else:
        print("WARNING: writing to S3")
        queryOut.coalesce(1).write.csv("s3a://csci.e-88.principles.of.big.data.processing/spark_queries/hw5_problem5_emr.csv")

    print("DONE")

    #-----------
    
    spark.stop()
