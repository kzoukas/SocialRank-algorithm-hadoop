package gr.tuc.softnet;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		double[] user_rank = new double[2];
		
		int i = 0;
		for (Text val : values) {
			
			user_rank[i] = Double.parseDouble(val.toString()); // vazoume ston pinaka mas(pou einai 2 8esewn) ta 2 ranks gia na upologisoume tin diafora tous
			i++;
			
		}

		double rank_diff = Math.abs(user_rank[0] - user_rank[1]);   //Ypologizoume tin diafora tous

		context.write(new Text(rank_diff + ""), new Text());         //grafoume tin diafora pou omws einai double giauto pros8etoume to ""

	}
}
