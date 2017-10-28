package com;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//	Main Class to run the task
public class Task2 {
	@SuppressWarnings("deprecation")
	//Main Method which creates the jobs
	public static void main(String[] args) throws Exception {
		Job job = new Job();								//	Creating a SalesOverCompany job
		job.setJarByClass(Task2.class);						//	Setting the Jar name
		job.setJobName("SalesOverCompany");					//	Giving the Job Name
		
		job.setMapperClass(Task2Mapper.class);				//	Setting the Mapper Class
		job.setReducerClass(Task2Reducer.class); 			//	Setting the Reducer Class

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); //File input path
		FileOutputFormat.setOutputPath(job,new Path(args[1]));//File output path
		
		job.waitForCompletion(true);
		
		Job job2 = new Job();								//	Creating a SalesOverCompanyState job
		job2.setJarByClass(Task2.class);					//	Setting the Jar name
		job2.setJobName("SalesOverCompanyState");			//	Giving the Job Name
		
		job2.setMapperClass(Task3Mapper.class);				//	Setting the Mapper Class
		job2.setReducerClass(Task3Reducer.class); 			//	Setting the Reducer Class

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		 
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[0])); 			//File input path
		//	Note : Appended "Job2" to the O/P path folder name as of multiple Jobs
		FileOutputFormat.setOutputPath(job2,new Path(args[1]+"Job2"));	//File output path
		
		job2.waitForCompletion(true);
	}
}
