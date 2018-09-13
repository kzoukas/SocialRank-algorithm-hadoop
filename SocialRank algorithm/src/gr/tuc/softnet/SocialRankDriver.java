package gr.tuc.softnet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import gr.tuc.softnet.*;


public class SocialRankDriver {

  public static void main(String[] args) throws Exception {
	  String jobs="";
	  jobs = args[0];
	  if (jobs.equals("init")) 
		{
		  init(args[1], args[2], Integer.parseInt(args[3]));
		  
		}
	  else if (jobs.equals("iter")) {

		  iter(args[1], args[2], Integer.parseInt(args[3]));
			
  
		}else if (jobs.equals("diff")){
			
			diff(args[1], args[2],args[3], Integer.parseInt(args[4]));
			
		}else if (jobs.equals("finish")) {
				
			finish(args[1], args[2], Integer.parseInt(args[3]));
						
					
		} else if (jobs.equals("composite")) {

			composite(args[1], args[2], args[3], args[4], args[5], Double.parseDouble(args[6]), Integer.parseInt(args[7]));
			
		}
		else{
		  System.err
			.println("Wrong job name");
	  }
  }
  
  static void init(String input, String output, int reducers) throws IOException, ClassNotFoundException, InterruptedException {
	
		
				System.out.println("Init Job!");
				Job job = Job.getInstance(); 
				job.setJarByClass(SocialRankDriver.class);
				job.setNumReduceTasks(reducers); 

				FileInputFormat.addInputPath(job, new Path(input));
				FileOutputFormat.setOutputPath(job, new Path(output));

				job.setMapperClass(InitMapper.class); 
				job.setReducerClass(InitReducer.class);

				job.setMapOutputKeyClass(Text.class); 
				job.setMapOutputValueClass(Text.class);

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				System.out.print(job.waitForCompletion(true) ? "End of Init Job\n"
						: "Init Job Error");
	  
  }
  static void iter(String input, String output, int reducers) throws IOException, ClassNotFoundException, InterruptedException {
		
			
		  System.out.println("Iter Job!");
			Job job = Job.getInstance();
			job.setJarByClass(SocialRankDriver.class); 
			job.setNumReduceTasks(reducers); 
	
			FileInputFormat.addInputPath(job, new Path(input)); 
			FileOutputFormat.setOutputPath(job, new Path(output));
	
			job.setMapperClass(IterMapper.class); 
			job.setReducerClass(IterReducer.class);
	
			job.setMapOutputKeyClass(Text.class); 
			job.setMapOutputValueClass(Text.class);
	
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
	
			System.out.print(job.waitForCompletion(true) ? "End of Iter Job\n"
					: "Iter Job Error");
			  
		  }
  
  static void diff(String input1,String input2, String output, int reducers) throws IOException,Exception, ClassNotFoundException, InterruptedException {
		
			
		  System.out.println("Diff1 Job! ");
			Job job = Job.getInstance(); 
			job.setJarByClass(SocialRankDriver.class); 
			job.setNumReduceTasks(reducers); 
	
			FileInputFormat.addInputPath(job, new Path(input1)); 
			FileInputFormat.addInputPath(job, new Path(input2)); 
			FileOutputFormat.setOutputPath(job, new Path("temporary")); //kratame se ena temporary file tis diafores twn ranks
	
			job.setMapperClass(DiffMap1.class); 
			job.setReducerClass(DiffRed1.class);
	
			job.setMapOutputKeyClass(Text.class); 
			job.setMapOutputValueClass(Text.class);
	
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
	
			if (job.waitForCompletion(true)) 
			{
				System.out.println("End of Diff1, Diff2 now");
				Job job1 = Job.getInstance(); 
				job1.setJarByClass(SocialRankDriver.class); 
				job1.setNumReduceTasks(reducers); 
	
				FileInputFormat.addInputPath(job1, new Path("temporary")); 
				FileOutputFormat.setOutputPath(job1, new Path(output)); 
	
				job1.setMapperClass(DiffMap2.class); 
				job1.setReducerClass(DiffRed2.class);
	
				job1.setMapOutputKeyClass(Text.class); 
				job1.setMapOutputValueClass(Text.class);
	
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(Text.class);
	
				System.out.print(job1.waitForCompletion(true) ? "End of Diff Job\n"
								: "Diff Job Error");
				
				deleteDirectory("temporary");
				
			
			}
  }
  static void finish(String input, String output, int reducers) throws IOException, ClassNotFoundException, InterruptedException {
		
			
	  				System.out.println("Finish Job!");
					Job job = Job.getInstance(); 
					job.setJarByClass(SocialRankDriver.class); 
					job.setNumReduceTasks(reducers);

					FileInputFormat.addInputPath(job, new Path(input)); 
					FileOutputFormat.setOutputPath(job, new Path(output));

					job.setMapperClass(FinishMapper.class); 
					job.setReducerClass(FinishReducer.class);

					job.setMapOutputKeyClass(DoubleWritable.class); 
					job.setMapOutputValueClass(Text.class);

					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(Text.class);

					
					System.out.print(job.waitForCompletion(true) ? "End of Finish Job\n"
							: "Finish Job Error");
		  
	  }
  public static void composite(String input, String output, String interim1,String interim2, String diff, double threshold, int reducers) throws Exception {
	  
		  System.out.println("Composite Job Started");
		  init(input,interim1,reducers);                    //arxika trexoume tin init
		  
		  double difference=10000;
		  int i=0;
		  int repeats=1;                                 //epanalhpseis. Arxikopoihsh sto 1 logw ths init
		  
		  while (difference > threshold)
		  {
			  
			  
			  if(i % 2 == 0){                          //an to i einai artios tote i iter pairnei san eisodo tin interim1 diladi tin eksodo ths init
				  
				  iter(interim1,interim2,reducers);
				  repeats++;
			  
			  }else {                                   //an einai perittos tote i iter pairnei san eisodo tin interim2 diladi tin eksodo ths proigoumenhs epanalhpshs tis iter
				  iter(interim2,interim1,reducers);
				  repeats++;
				 
			  }
			  
			  if (i % 3 == 0){                       //se ka8e 3 epanalhpseis elegxoume thn diafora me thn diff job
				  
				  diff(interim1,interim2,"difftemp", reducers);
				  			repeats++;
							Path diffp = new Path("difftemp");                      //oi parakatw grammes kwdika ousiastika sikwnoun tin megisth diafora apo to apotelesma
							Configuration conf = new Configuration();				//ths diff h opoia mpainei ws kainouria timh ths difference etsi wste na th sugrinoume
							FileSystem fs = FileSystem.get(URI.create("difftemp"), conf);    //stin epomeni epanalipsi me to threshold
		
							if (fs.exists(diffp)) {
								FileStatus[] ls = fs.listStatus(diffp);
								for (FileStatus file : ls) { 
									
									if (file.getPath().getName().startsWith("part-r-00")) {
										FSDataInputStream diffin = fs.open(file.getPath());
										BufferedReader d = new BufferedReader(
												new InputStreamReader(diffin));
										String diffcontent = d.readLine();
										if (diffcontent != null) {
											double diff_temp = Double.parseDouble(diffcontent);
											difference = diff_temp;                 
											
											d.close();
										}
									}
								}
							}
		
							fs.close();
							deleteDirectory("difftemp");
			 }
			  if(i % 2 == 0){                     //se ka8e epanalipsi analoga me to an i eksodos ths iter einai i interim1 h i interim2
				  									//diagrafoume to antistoixo gia na min uparxei sthn epomenh epanalipsi
				  deleteDirectory(interim1);
			  
			  }
			  if(i % 2 == 1){
				  deleteDirectory(interim2);
			  }
			  
			  
			  i++;
		  }
		  if (i % 2 == 1)       // otan teleiwsoun oi epanalipseis kai exoume tis diafores kaloume tin finish gia na paroume to teliko apotelesma
			{
				deleteDirectory(interim1); 
				finish(interim2, output, reducers);
				 repeats++;
				
			} else 
			{
				deleteDirectory(interim2); 
				finish(interim1, output, reducers);
				 repeats++;
				
			}
		  System.out.println(repeats);
		
		
		
	  
	  
  }
  
  static void deleteDirectory(String path) throws Exception {
		Path todelete = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(todelete))
			fs.delete(todelete, true);

		fs.close();
	}
  
 
  
}