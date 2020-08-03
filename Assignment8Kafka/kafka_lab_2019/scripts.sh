docker-compose -f kafka-docker-compose.yaml up -d --scale kafka=3
docker exec kafka_lab_kafka_1 kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 2 --partitions 5 --topic test_topic
docker exec -t kafka_lab_python-example_1 python /python/python-producer.py
docker exec  kafka_lab_python-example_1 python /python/python-consumer.py
