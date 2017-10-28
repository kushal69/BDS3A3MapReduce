package com;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 
//	Mapper Class
public class Task3Mapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		//	Making the array of elements of the input from dataset by splitting
		String[] lineArray = value.toString().split("\\|");	
		
		Text companyName = new Text(lineArray[0]);
		Text productName = new Text(lineArray[1]);

		//	Writing the Output based on the Condition
		if(!(companyName.toString().equalsIgnoreCase("NA")) &&
				!(productName.toString().equalsIgnoreCase("NA"))) {
			context.write(companyName, value);
		}
	}
}
