# Using Kafka API, create a new Problem3Producer application that generates and sends events to the "problem3" topic - generate the same type of log events we've worked with so far, plus add a UUID:
# one event is one line in the format: <uuid> <timestamp> <url> <userId>
# make sure you have at least 3 different userIDs

from kafka import KafkaProducer
import time
import random
import uuid

producer = KafkaProducer(bootstrap_servers='34.201.113.133:9092')

# debug only
# producer.send('problem3', key=str.encode(str(1)), value=str.encode(str(2))) #this is async
# producer.send('problem3', key=b'foo', value=b'bar') #this is async
# while True:
#     time.sleep(1)

userList = ['Loi','Mao','Jesus']
urlList = ['www.awesome.cow','somthing.onion','peacefulriot.org']


var = 1
while var == 1 :

    #build message
    newUUID = uuid.uuid1()
    userId = random.choice(userList)
    timestamp = time.time()
    newUrl = random.choice(urlList)
    msg = str(newUUID) +':' +str(timestamp) +':' +newUrl +':' +userId
    msg = str.encode(msg)
    print(msg)
    #send message
    producer.send('lab9', value=msg, key=b'pythonproduce')
    print('message sent')
    #set partition
    time.sleep(1/200.0)