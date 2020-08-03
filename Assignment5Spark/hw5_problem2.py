from __future__ import print_function
import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Spark
    """
    spark = SparkSession\
        .builder\
        .appName("WriteParquet")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    #----------
    #

    #run on s3 file no args specified, else run on local files
    if len(sys.argv) > 1:
        print("WARNING: reading from local filesystem")
        df = spark.read.format("csv").load("input_files/*.csv")
        df.write.parquet("hw5_problem2.parquet")
    
    else:
        print("WARNING: reading from S3")
        df = spark.read.format("csv").load("s3a://csci.e-88.principles.of.big.data.processing/input_files/*.csv")
        df.write.parquet("s3a://csci.e-88.principles.of.big.data.processing/hw5_problem2.parquet")

    print("DONE")

    #-----------
    
    spark.stop()