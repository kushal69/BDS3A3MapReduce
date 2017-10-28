package com;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//	Reducer Class
public class Task2Reducer extends Reducer<Text, Text, Text, Text>
{	
	//	reduce method which implements the logic to process the data set
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		//	Hash Map to store the values during processing
		HashMap<String, Integer> soldUnitsList = new HashMap<String, Integer>();
		
		//	Loop to iterating over the data-set
		for (Text value : values) {
			String [] record = value.toString().split("\\|");
			
			if(soldUnitsList.containsKey(record[0])){
				soldUnitsList.put(record[0], 
						soldUnitsList.get(record[0]) + 1);
			} else {
				soldUnitsList.put(record[0], 1);
			}
		}
		
		//	To display the processed output
		for(Entry<String, Integer> map : soldUnitsList.entrySet()) {
			context.write(new Text(map.getKey()), 
					new Text(map.getValue().toString()));
		}
	}
}
