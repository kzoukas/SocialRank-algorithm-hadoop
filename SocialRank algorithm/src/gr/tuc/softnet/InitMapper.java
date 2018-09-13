package gr.tuc.softnet;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import gr.tuc.softnet.InitReducer;


public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,IllegalArgumentException {
		
	
		String input_line = value.toString(); 
		
		if(input_line.contains("\t")){   //spame thn eisodo
			
			String[] values = input_line.split("\t");
			context.write(new Text(values[0]), new Text(values[1]));
		} else {
			throw new IllegalArgumentException("Wrong input");
		
		}
	}	
}		
	
