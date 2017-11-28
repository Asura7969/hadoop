package org.hadoop.mapreduce.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class RunJob {

	public static void main(String[] args) {
		
		Configuration config =new Configuration(false);
		config.set("fs.defaultFS", "hdfs://192.168.217.11:8020");
		Job job;
		try {
			job = Job.getInstance(config);
			FileSystem fs =FileSystem.get(config);
			job.setJobName("weather");
			job.setJarByClass(RunJob.class);
			
			job.setMapperClass(WeatherMapper.class);
			job.setReducerClass(WeatherReducer.class);
			
			job.setMapOutputKeyClass(MyKey.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setGroupingComparatorClass(MyGroup.class);
			job.setCombinerClass(MyCombiner.class);
			job.setCombinerKeyGroupingComparatorClass(MyCombinerGroupComparator.class);
			job.setPartitionerClass(MyPartitioner.class);
			job.setNumReduceTasks(3);//三个reduce，每年分一个
			
			//重新设置一个InputFormat
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			
			FileInputFormat.addInputPath(job, new Path("/input/weather"));
			
			Path outdir =new Path("/output/weather");
			if(fs.exists(outdir)){
				fs.delete(outdir, true);
			}
			
			FileOutputFormat.setOutputPath(job, outdir);
			
			boolean f =job.waitForCompletion(true);
			if(f){
				System.out.println("Job 成功执行");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static class MyPartitioner extends HashPartitioner<MyKey, Text>{
		
		public int getPartition(MyKey key, Text value, int numReduceTasks) {
			return key.getYear() % numReduceTasks;
		}
	}
}
