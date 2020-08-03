package org.cscie88.kafka;

import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
	private Properties props = new Properties();
	KafkaConsumer<String, String> consumer;

	public SimpleConsumer(String topicName) {
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		consumer = new KafkaConsumer<String, String>(props);

		// subscribe to a list of topics
		consumer.subscribe(Arrays.asList(topicName));
		logger.info("SimpleConsumer initialized Ok and subscribed to topic: {}", topicName);
	}

	public void run(String topicName) {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100l));
			if (records.isEmpty()) {
				logger.info("called poll() - no records read");
			} else {
				logger.info("called poll() - processing records ...");				
			}
			for (ConsumerRecord<String, String> record : records) {
				// print the offset,key and value for the consumer records.
				logger.info("Recieved event: offset = {},partition = {},  key = {}, value = {}\n", record.offset(),
						record.partition(), record.key(), record.value());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Enter topic name");
			return;
		}
		// Kafka consumer topic
		String topicName = args[0].toString();
		SimpleConsumer simpleConsumer = new SimpleConsumer(topicName);
		simpleConsumer.run(topicName);
	}

}