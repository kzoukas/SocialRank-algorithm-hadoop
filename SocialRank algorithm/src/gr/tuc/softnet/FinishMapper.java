package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,IllegalArgumentException {
		
		String line = value.toString(); 
		
		String[] sections = line.split("\t"); 
		
		String[] parts = sections[0].split("rank:");
		
		Double rank = Double.parseDouble(parts[1]);
		
		context.write(new DoubleWritable(-rank), new Text(parts[0])); 

		
	}

}
