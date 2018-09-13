package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class FinishReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
		Double rank = -Double.parseDouble(key.toString());
		for (Text v : values) 
		{
			context.write(v, new Text(rank + "")); 
		}

	}
}