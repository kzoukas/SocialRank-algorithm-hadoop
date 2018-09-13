package gr.tuc.softnet;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {


	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = 0.15; // Decay factor
		double new_rank = 0.0; // stores the decay factor in a variable rank

	

		String followList = "";
		double sum = 0.0;
			for (Text val : values)                          // Ypologizoume to a8roisma olwn twn r/N , dhladh olwn aytwn pou akolou8ei o user mas
			{
				String temp = val.toString();
				if (temp.contains("follow_list")) {         //Ama vrei to follow_list pou valame ston mapper sto telos 
					followList = temp.substring(11);		// tou leme na diagrapsei ta 11 grammata(follow_list)
				} else {
					double rank_div_N = Double.parseDouble(temp); 
					sum = sum + (rank_div_N); 
				}
			}
		new_rank = d + (1 - d) * sum;
		context.write(new Text(key.toString() + "rank:" + new_rank), new Text(followList)); //To idio me prin mono pou twra exoume to neo rank
	
	}
}

