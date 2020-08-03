package edu.harvard.e88.lab5;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;

public class HoursCounterSparkJobParquet {
	
	
	public static void main(String[] args) throws IOException {
		
		if (args.length < 1) {
			System.out.println("Usage: HoursCounterSparkJobParquet <input_dir>");
			System.exit(0);			
		}
	
		String inputDirectory = args[0];
		
		//Create Spark Session
		SparkSession spark = SparkSession
				  .builder()
				  .master("local[*]")
				  .appName("HoursCounterSparkJobParquet")
				  .getOrCreate();
		
		
		// Creates a DataFrame from an parquet files.
		Dataset<Row> dfparquet = spark.read().parquet(inputDirectory);	

		
		//Get hourly counts
	    JavaPairRDD<String, Long> counts = dfparquet
				.toJavaRDD()
				.map(s -> s.getString(1))
				.mapToPair(s -> new Tuple2 <String, Long> (s.substring(0, 13), 1L))
				.reduceByKey((a, b) -> a + b);
				
		
	      // instantiate Configuration class
	      Configuration config = HBaseConfiguration.create();
	      Connection connection = ConnectionFactory.createConnection(config);
	      
	      System.out.println(connection.isClosed());
	      
	      // instantiate Table class
	      
	      //Table table = connection.getTable(TableName.valueOf("lab6:log"));
	      Table table = connection.getTable(TableName.valueOf("lab:date_hour"));
	      
	      for (Tuple2<String, Long> l : counts.collect())
	      {

	    	  	table.put(new Put(Bytes.toBytes(l._1))
	    	  		.addColumn(Bytes.toBytes("url"),
	    	  				Bytes.toBytes("count"),
	    	  	Bytes.toBytes(l._2.toString())));

	      }

	      // close Table instance
	      table.close();
	    
	      //close connection
	      connection.close();
	    
		 //counts.saveAsTextFile("jobs_output_parquet_new_p");

	}
}