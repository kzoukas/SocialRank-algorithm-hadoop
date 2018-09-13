package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,IllegalArgumentException {
		
		
		
		String input_line = value.toString(); 
		
		String[] line_data = input_line.split("\t");      //Spame thn grammh  sto \t
		if (line_data.length != 2) {
			return;
		}
		String[] user_rank = line_data[0].split("rank:"); //Spame to prwto stoixeio ths grammhs sto rank:
		
		double rank = Double.parseDouble(user_rank[1]);   //Kanoume double to rank gt meta thn diairesh me to N(j) 8a paroume apotelesma se auth th morfh
		
		String[] follow_friend = line_data[1].split(","); //Spame to deutero stoixeio ths grammhs sto ,
		
		int N = follow_friend.length;                     //Upologizoume to N(j)
		
		double rank_div_N = (rank / N); 				  //Upologizoume thn diaresh r/N

		String val = rank_div_N + ""; 					  //Fernoume to rank se string morfh
		
		for (int i = 0; i < N; i++) 					  // Epanalipsi pou antistoixizei to rank pou upologisame se olous autous pou akolou8ei o user mas
		{
			context.write(new Text(follow_friend[i]), new Text(val));
		}

		context.write(new Text(user_rank[0]), new Text("follow_list" + line_data[1]));  // <key , follow_list follow_1,..,follow_n>  vazw to follow_list mprosta
																						//gia na ksexwrizei apo ta alla kai na to vrw ston reducer meta

	}

}
