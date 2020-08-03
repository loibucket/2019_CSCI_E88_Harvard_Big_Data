package org.cscie88.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer {

	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
	private Properties props = new Properties();
	private Producer<String, String> producer;

	public SimpleProducer() {
		// Assign localhost id
		props.put("bootstrap.servers", "localhost:9092");
		// Set acknowledgements for producer requests.
		props.put("acks", "all");
		// If the request fails, the producer can automatically retry,
		props.put("retries", 0);
		// Specify buffer size in config
		props.put("batch.size", 16384);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);
		logger.info("SimpleProducer initialized Ok");
	}

	public void produceEvents(int numberOfEvents, String topicName) {
		for (int i = 0; i < numberOfEvents; i++) {
			String messageBody = "marina_" + i;
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), messageBody));
			logger.info("Produced message successfully: {}", messageBody);
		}
		producer.close();
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Enter topic name");
			return;
		}
		String topicName = args[0].toString();
		SimpleProducer simpleProducer = new SimpleProducer();
		simpleProducer.produceEvents(10, topicName);
	}

}