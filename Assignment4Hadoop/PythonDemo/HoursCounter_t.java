package edu.harvard.e88.lab4;

import java.io.File;
import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
//import org.cscie88.LogLine1;
//import org.cscie88.LogLineParser1;

/**
 * 
 *
 */
public class HoursCounter_t
{
	//HoursMapper is a subclass of Mapper Key and is passed AvroKey as it's key
	public static class HoursMapper extends
     Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

	    //Split the line in tokens
	    	String line = value.toString();	
	    	String[] tokens = line.split(",");
			
			

        context.write(new Text(tokens[1].toString().substring(0,13)), new IntWritable(1));
   }
 }
	
	public static class HoursReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			//System.out.println("reduce result for url: " + key + ": " + sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		//org.apache.log4j.BasicConfigurator.configure();
		if (args.length < 2) {
			System.out.println("Usage: HoursCounter text <input_dir> <output_dir>");
			System.exit(0);			
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "hours click counter text");
		job.setJarByClass(HoursCounter.class);
	//	job.setInputFormatClass(AvroKeyInputFormat.class);		
		job.setMapperClass(HoursMapper.class);
		job.setCombinerClass(HoursReducer.class);
		job.setReducerClass(HoursReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileUtils.deleteQuietly(new File(args[1]));
		
		//FileUtils.deleteQuietly(new File(args[1]));
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (job.waitForCompletion(true)) {
			System.out.println("I am DONE OK !");
			System.exit(0);
		} else {
			System.out.println("I am NOT DONE OK !");
			System.exit(1);
		}
	}
}
