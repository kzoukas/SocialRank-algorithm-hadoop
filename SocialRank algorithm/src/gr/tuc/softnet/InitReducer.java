package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class InitReducer extends Reducer<Text, Text, Text, Text> {


	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String user = key.toString(); 
		int rank = 1; 
		user = user + "rank:" + rank; 
		String followList = "";
		for (Text value : values)       //vazoume se mia lista olous tous followers tou xrhsth key kai tous xwrizoume me komma
		{
			followList = followList + value.toString() + ","; 
		}
		context.write(new Text(user), new Text(followList.substring(0, followList.lastIndexOf(',')))); //vgazoume to teleutaio komma

	}
}