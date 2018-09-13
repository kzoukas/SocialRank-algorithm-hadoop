package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap2 extends Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,IllegalArgumentException {
		
		String rank_diff = value.toString(); 

		context.write(new Text("Diff"), new Text(rank_diff)); 

	}

}
