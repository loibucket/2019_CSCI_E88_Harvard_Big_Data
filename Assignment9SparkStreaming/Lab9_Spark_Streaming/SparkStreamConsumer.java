package edu.harvard.e88.lab9spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.Optional;
//import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka010.*;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SparkStreamConsumer {
    public static void main(String[] args) {
    	
		Function2<List<Long>, Optional<Long>, Optional<Long>>
		  runningSum = (nums, current) -> {
		     long sum = current.or(0L);
		     for (long i : nums) {
		       sum += i;
		     }
		     return Optional.of(sum);
		   };

		if (args.length < 2) {
				System.out.println("Usage: SparkStreamConsumer <brokers> <topics>");
				System.exit(0);			
		}

			String topicName = args[0];
			String brokers = args[1];   
		   
// Create Spark Context			
        SparkConf conf = new SparkConf()
                .setAppName("kafka-logs");
  //              .setMaster("local[*]");
        
      //  conf.set("spark.streaming.concurrentJobs","4");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(5000));
        
	    // Checkpointing must be enabled to use the updateStateByKey function.
	    jssc.checkpoint("log");


//	    Define the input sources by creating input DStreams
	    
        Map<String, Object> kafkaParams = new HashMap<>();
        
        Set<String> topics = Collections.singleton(topicName);
        
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);
  //      kafkaParams.put("spark.streaming.kafka.consumer.cache.enabled","false");


        sc.setLogLevel("WARN");
      
        
        JavaInputDStream<ConsumerRecord<String, String>> stream =
        		  KafkaUtils.createDirectStream(
        		    jssc,
        		    LocationStrategies.PreferConsistent(),
        		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        		  );

//Define the streaming computations by applying transformation and output operations to DStreams
        
        // Get the lines
        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
        
		JavaPairDStream <String, Long> batchTuples = lines.mapToPair(s -> {
			String [] tokens = s.split(",");
			String key = tokens[1].substring(0,13);
			Long value = 1L;
			return new Tuple2 <>(key, value );	
		});
		
    //    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Long> counts = batchTuples.reduceByKey((a,b) -> a+b );
        
      //  JavaPairDStream<String, Long> counts1 = batchTuples.reduceByKeyAndWindow((a,b) -> a+b,  Durations.seconds(10), Durations.seconds(10));
        JavaPairDStream<String, Long> runningCounts = batchTuples.updateStateByKey(runningSum);
 
        
        batchTuples.reduceByKeyAndWindow((a,b) -> a+b,  Durations.seconds(10), Durations.seconds(10)).print();
        
        
       // batchTuples.reduce
      //  counts1.print();
     //   counts.print();
     //   runningCounts.print();

        //Start receiving data and processing 
        // Start the computation
        jssc.start();
        
	    try {
    	  	// Wait for the computation to terminate
	    		jssc.awaitTermination();
	    } catch (InterruptedException e) {
	    		e.printStackTrace();
	    }
	    finally
	    {
	    		jssc.stop();
	    }
        
    }

}
