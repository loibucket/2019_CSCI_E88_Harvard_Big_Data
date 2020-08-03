package edu.harvard.e88.lab9;


import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.sql.Timestamp;
import java.time.Instant;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WeblogsKafkaProducer {

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println("Usage: WeblogsKafkaProducer <topic> <brokers>");
			System.exit(0);
		}

		String topicName = args[0];
		String brokers = args[1];

		Random rnd = new Random();
		Properties props = new Properties();
		//props.put("metadata.broker.list", brokers);
    props.put("bootstrap.servers", brokers);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("producer.type", "async");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new KafkaProducer
			         <String, String>(props);


	  //   for(int i = 0; i < 10; i++) {
		while (true) {
	    	  	String uuid = UUID.randomUUID().toString().replace("-", "");
	    	  	String tm = new Timestamp(System.currentTimeMillis()).toInstant().plusSeconds(3600*rnd.nextInt(24)).toString();
	    	  	//String tm = Instant.now().plusHours(1);
	    	  	String url = "http://example.com/?url=" + rnd.nextInt(255);
	    	  	String user = "user-"+ rnd.nextInt(255);
	    	  	String rg = "GQ";
	    	  	String browser = "Chrome";
	    	  	String platform = "Windows";
	    	  	String cd = "200";
	    	  	String ttf = String.valueOf(Math.random());

	    	  	String msg_data = uuid+","+tm+","+url+","+user+","+rg+","+browser+","+platform+","+cd+","+ttf;

	        producer.send(new ProducerRecord<String, String>(topicName,
	        		uuid, msg_data));

	        System.out.println("Message sent successfully: " + msg_data);
	        try
	        {
	            Thread.sleep(5);
	        }
	        catch(InterruptedException ex)
	        {
	            Thread.currentThread().interrupt();
	  	        producer.close();
	        }
	      }
	    }


}
