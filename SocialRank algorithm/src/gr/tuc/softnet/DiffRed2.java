package gr.tuc.softnet;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		double max = Double.MIN_VALUE;
		
		int i = 0;
		
		for (Text val : values) {          //psaxnoume na vroume to max
			
			double temp = Double.parseDouble(val.toString()); 
			if (temp > max) {
					max = temp;          //Ean i timh mas einai megaluterh tou max ginetai auth max
			}
		}

		context.write(new Text(max + ""), new Text());  //grafoume to max

	}
}
