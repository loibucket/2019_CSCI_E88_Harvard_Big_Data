from kafka import KafkaProducer
import time
import random

producer = KafkaProducer(bootstrap_servers='kafka:9092')

var = 1
while var == 1 :
    num = random.randint(0,10)
    print str(num)
    producer.send('test_topic' ,value=str(num), key=str(num) )
    time.sleep(1)