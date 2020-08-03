import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import os

if __name__ == "__main__":
    # Create Spark Context
    sc = SparkContext(appName="PythonStreamingDirectKafkaCount")
    ssc = StreamingContext(sc, 1)

    brokers, topic, accuracy = sys.argv[1:]

    print(brokers)
    print(topic)

    sc.setLogLevel("WARN")

    #Create a DStream that will connect to Kafka
    #kafkaParams = {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}
    kafkaParams = {"metadata.broker.list": brokers}
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)

    def parse_log_line(line):
        (uuid, timestamp, url, user) = line.strip().split(":")
        return (user, 1)

    lines = kafkaStream.map(lambda x: x[1]).window(30,30)

    def approxDistinct(time,rdd):
        print("Time: %s count approx distinct..." % str(time))
        try:
            print('count approx distinct',rdd.countApproxDistinct(float(accuracy)))
        except:
            pass

    #approximate
    lines.foreachRDD(approxDistinct)

    def countActual(time,rdd):
        print("Time: %s count distinct..." % str(time))
        try:
            print('count distinct',rdd.count())
        except:
            pass

    #actual
    clicks = lines.map(parse_log_line).reduceByKey(lambda a, b: a + b)
    #clicks.pprint()
    clicks.foreachRDD(countActual)

    # Start the computation
    ssc.start()
    # Wait for the computation to terminate
    ssc.awaitTermination()