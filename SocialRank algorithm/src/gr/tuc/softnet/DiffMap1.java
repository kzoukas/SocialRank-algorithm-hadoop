package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,IllegalArgumentException {
		
		String line = value.toString();
		
		String[] line_data = line.split("\t");  //Spame thn grammh  sto \t
		
		String[] node_rank = line_data[0].split("rank:"); //Spame to prwto stoixeio ths grammhs sto rank:

		context.write(new Text(node_rank[0]), new Text(node_rank[1]));  //     output:   <key,rank>

	}

}
