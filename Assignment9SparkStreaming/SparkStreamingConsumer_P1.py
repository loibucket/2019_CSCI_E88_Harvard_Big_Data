import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import os

if __name__ == "__main__":
    # Create Spark Context
    sc = SparkContext(appName="PythonStreamingDirectKafkaCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint")

    brokers, topic = sys.argv[1:]

    print(brokers)
    print(topic)

    sc.setLogLevel("WARN")

    #Create a DStream that will connect to Kafka
    #kafkaParams = {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}
    kafkaParams = {"metadata.broker.list": brokers}
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)

    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([])

    def updateFunc(new_value, last_sum):
        return sum(new_value) + (last_sum or 0)

    def parse_log_line(line):
        (uuid, timestamp, url, user) = line.strip().split(":")
        return (url, 1)

    lines = kafkaStream.map(lambda x: x[1])

    clicks = lines.map(parse_log_line)\
        .reduceByKey(lambda a, b: a + b)
    clicks.pprint()

    running_counts = clicks.updateStateByKey(updateFunc, initialRDD=initialStateRDD)
    running_counts.pprint()

    # Start the computation
    ssc.start()
    # Wait for the computation to terminate
    ssc.awaitTermination()
