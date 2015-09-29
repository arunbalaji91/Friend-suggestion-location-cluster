import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MapReduceJobs {
     static String OutputFileName = "part-r-00000";
     static Integer iteration = 0,userId;
     static String centroid=null;
     static ArrayList<String> topFriendsList = new ArrayList<String>();
	public static double calc_distance(double lat1, double lon1, double lat2,
			double lon2) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
				* Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		dist = dist * 1.609344;
		return (dist);
	}

	public static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	public static double rad2deg(double rad) {
		return (rad * 180 / Math.PI);
	}

	public static void main(String[] args) throws Exception {
		String CENTROID_FILE_NAME = "Input/Centroid.txt";
		boolean isdone = false;
		FileWriter start_centroid_file=null;
		FileWriter fw=null;
		FileReader inputfile=null;
		userId=Integer.parseInt(args[1]);
		 try
		 {
			    start_centroid_file = new FileWriter(CENTROID_FILE_NAME,false);
			    inputfile = new FileReader("Input/Brightkite_totalCheckins.txt");
				BufferedReader br = new BufferedReader(inputfile);
				BufferedWriter out = new BufferedWriter(start_centroid_file);
				String line = br.readLine();
				int centroidCount=0;
				while (centroidCount != 200) {
					String c = line + "::";
		            out.write(c);
		            out.newLine();
		            centroidCount++;
		            line = br.readLine();
				} 
				br.close();
				out.close();
		 }
		 catch(Exception ex)
		 {
			 	System.out.println(ex);
		 }
		 finally
		 {
			 if (inputfile != null)
			  {
			   try { inputfile.close(); } 
			   catch (Exception e) { } 
			  }
			 inputfile=null;
			  if (start_centroid_file != null)
			  {
			   try { start_centroid_file.close(); } 
			   catch (Exception e) { } 
			  }
			  start_centroid_file=null;
		}
		while (isdone == false) {
			
		 if (iteration != 0)
			 {		
				 FileWriter cf = new FileWriter(CENTROID_FILE_NAME,false);
				 try
				 {
					 FileReader f1 = new FileReader("job_output" + (iteration-1) + "/"
								+ OutputFileName);
						BufferedReader br = new BufferedReader(f1);
						BufferedWriter out = new BufferedWriter(cf);
						String line = br.readLine();
						while (line != null) {
							String[] sp = line.split("::");
							String c = sp[0];
				            out.write(c);
				            out.newLine();
				            line = br.readLine();
						} 
			            br.close();
			            out.close();
				 }
				 catch(Exception ex)
				 {
					 	System.out.println(ex);
				 }
				 finally
				 {
					 cf.close();
				 }
			 } 

			Configuration conf = new Configuration();
			ControlledJob cjob1 = new ControlledJob(conf);
			 cjob1.setJobName("Map Reduce Stage2");
			 Job job = cjob1.getJob();
			// conf.setJobName(JOB_NAME);
			job.setJarByClass(MapReduceJobs.class);
			FileInputFormat.setInputPaths(job, args[0]);

			
			job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);

			// CG_MAPPER_HIDDEN
			job.setMapperClass(KMeansMapper.class);
			job.getConfiguration().set("mapred.mapper.new-api", "true");

			// CG_MAPPER
			job.getConfiguration().set("mapred.map.tasks", "3");
			job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
			job.setMapOutputValueClass(org.apache.hadoop.io.Text.class);

			// CG_PARTITIONER_HIDDEN
			job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);

			job.setReducerClass(KMeansReducer.class);
			job.getConfiguration().set("mapred.reducer.new-api", "true");

			// CG_REDUCER
			job.getConfiguration().set("mapred.reduce.tasks", "2");
			job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
			job.setOutputValueClass(org.apache.hadoop.io.Text.class);

			// CG_OUTPUT_HIDDEN
			job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path("job_output"+ iteration));

			// FileInputFormat.setInputPaths(conf,
			// new Path(input + DATA_FILE_NAME));
			// FileOutputFormat.setOutputPath(conf, new Path(output));

			// JobClient.runJob(conf);

			job.submit();
			job.waitForCompletion(true);

			FileReader f1 = new FileReader("job_output" + iteration + "/"
					+ OutputFileName);
			BufferedReader br = new BufferedReader(f1);

			// Path ofile = new Path();
			// FileSystem fs = FileSystem.get(new Configuration());
			// BufferedReader br = new BufferedReader(new InputStreamReader(
			// fs.open(ofile)));
			ArrayList<String> centers_next = new ArrayList<String>();
			String line = br.readLine();
			while (line != null) {
				String[] sp = line.split("::");
				String c = sp[0];
				centers_next.add(c);
				line = br.readLine();
			}
			br.close();

			String prev;
			if (iteration == 0) {
				prev = CENTROID_FILE_NAME;
			} else {
				prev = "job_output" + (iteration - 1) + "/" + OutputFileName;
			}
			FileReader f2 = new FileReader(prev);
			BufferedReader br1 = new BufferedReader(f2);
			ArrayList<String> centers_prev = new ArrayList<String>();
			String l = br1.readLine();
			while (l != null) {
				String[] sp1 = l.split("::");
				String c = sp1[0];
				centers_prev.add(c);
				l = br1.readLine();
			}
			br1.close();
			f2.close();
			ArrayList<String> sorted_centers_prev = new ArrayList<String>();
			for (String next_centroid : centers_next) {
				double min_dist = Double.MAX_VALUE;
				double curr_dist = Double.MAX_VALUE;
				String tempString = "";
				for (String prev_centroid : centers_prev) {
					curr_dist = calc_distance(Double.parseDouble(KMeansMapper
							.splittingFunction(next_centroid)[2]),
							Double.parseDouble(KMeansMapper
									.splittingFunction(next_centroid)[3]),
							Double.parseDouble(KMeansMapper
									.splittingFunction(prev_centroid)[2]),
							Double.parseDouble(KMeansMapper
									.splittingFunction(prev_centroid)[3]));
					if (curr_dist < min_dist) {
						min_dist = curr_dist;
						tempString = prev_centroid;
					}
				}
				sorted_centers_prev.add(tempString);
			}
			// Sort the old centroid and new centroid and check for convergence
			// condition
			double convergence_dist = Double.MAX_VALUE;
			for (int i = 0; i < centers_next.size(); i++) {
				convergence_dist = calc_distance(
						Double.parseDouble(KMeansMapper
								.splittingFunction(centers_next.get(i))[2]),
						Double.parseDouble(KMeansMapper
								.splittingFunction(centers_next.get(i))[3]),
						Double.parseDouble(KMeansMapper
								.splittingFunction(sorted_centers_prev.get(i))[2]),
						Double.parseDouble(KMeansMapper
								.splittingFunction(sorted_centers_prev.get(i))[3]));
				if (Math.abs(convergence_dist) < 300) {
					isdone = true;
				} else {
					isdone = false;
					break;
				}
			}
			++iteration;
			if(iteration==3) break;
		}//while loop
		//Looking for the nearest cluster centroid and storing it for further use
		FileReader f2 = new FileReader("job_output" + (iteration-1) + "/"+ OutputFileName);
		BufferedReader br1 = new BufferedReader(f2);
//		ArrayList<String> users_records = new ArrayList<String>();
		String l = br1.readLine();
		double min_dist=Double.MAX_VALUE;
	    centroid=null;
	    String selected_centroid=null;
		while (l != null)
		{
			String records[]=l.split("::");
		    centroid = records[0];
			double temp_dist=calc_distance(
					Double.parseDouble(args[2]),
					Double.parseDouble(args[3]),
					Double.parseDouble(KMeansMapper
							.splittingFunction(centroid)[2]),
					Double.parseDouble(KMeansMapper
							.splittingFunction(centroid)[3]));
			if(temp_dist<min_dist)
			{
				min_dist=temp_dist;
				selected_centroid=centroid;
			}
			 l = br1.readLine();
		}
		centroid=selected_centroid;
		br1.close();
		f2.close();
		
		
		
		Configuration conf2 = new Configuration();
		 ControlledJob cjob2 = new ControlledJob(conf2);
		 cjob2.setJobName("Map Reduce Stage2");
		 Job job2 = cjob2.getJob();
	    // cjob2.addDependingJob(cjob1);

		// conf.setJobName(JOB_NAME);
		job2.setJarByClass(MapReduceJobs.class);
		FileInputFormat.setInputPaths(job2,"Input/Brightkite_edges.txt");

		
		job2.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);

		// CG_MAPPER_HIDDEN
		job2.setMapperClass(Mutual_Friend_Mapper.class);
		job2.getConfiguration().set("mapred.mapper.new-api", "true");

		// CG_MAPPER
		job2.getConfiguration().set("mapred.map.tasks", "3");
		job2.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		job2.setMapOutputValueClass(org.apache.hadoop.io.Text.class);

		// CG_PARTITIONER_HIDDEN
		job2.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);

		job2.setReducerClass(Mutual_Friend_Reducer.class);
		job2.getConfiguration().set("mapred.reducer.new-api", "true");

		// CG_REDUCER
		job2.getConfiguration().set("mapred.reduce.tasks", "2");
		job2.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		job2.setOutputValueClass(org.apache.hadoop.io.Text.class);

		// CG_OUTPUT_HIDDEN
		job2.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job2, new Path("Mutual_Friend_Stage1"));

		// FileInputFormat.setInputPaths(conf,
		// new Path(input + DATA_FILE_NAME));
		// FileOutputFormat.setOutputPath(conf, new Path(output));

		// JobClient.runJob(conf);

		job2.submit();
		job2.waitForCompletion(true);
		
		
		
		Configuration conf3 = new Configuration();
		 ControlledJob cjob3 = new ControlledJob(conf3);
		 cjob3.setJobName("Map Reduce Stage2");
		 Job job3 = cjob3.getJob();
	    // cjob2.addDependingJob(cjob1);

		// conf.setJobName(JOB_NAME);
		job3.setJarByClass(MapReduceJobs.class);
		FileInputFormat.setInputPaths(job3,"Mutual_Friend_Stage1/"+OutputFileName);

		
		job3.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);

		// CG_MAPPER_HIDDEN
		job3.setMapperClass(Mutual_Friend_Mapper_Stage2.class);
		job3.getConfiguration().set("mapred.mapper.new-api", "true");

		// CG_MAPPER
		job3.getConfiguration().set("mapred.map.tasks", "3");
		job3.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		job3.setMapOutputValueClass(org.apache.hadoop.io.Text.class);

		// CG_PARTITIONER_HIDDEN
		job3.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);

		job3.setReducerClass(Mutual_Friend_Reducer_Stage2.class);
		job3.getConfiguration().set("mapred.reducer.new-api", "true");

		// CG_REDUCE
		job3.getConfiguration().set("mapred.reduce.tasks", "2");
		job3.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		job3.setOutputValueClass(org.apache.hadoop.io.Text.class);

		// CG_OUTPUT_HIDDEN
		job3.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job3, new Path("Mutual_Friend_Stage2"));

		// FileInputFormat.setInputPaths(conf,
		// new Path(input + DATA_FILE_NAME));
		// FileOutputFormat.setOutputPath(conf, new Path(output));

		// JobClient.runJob(conf);

		job3.submit();
		job3.waitForCompletion(true);
		
		
		
		Configuration conf4 = new Configuration();
		 ControlledJob cjob4 = new ControlledJob(conf4);
		 cjob4.setJobName("Map Reduce Stage2");
		 Job job4 = cjob4.getJob();
	    // cjob2.addDependingJob(cjob1);

		// conf.setJobName(JOB_NAME);
		job4.setJarByClass(MapReduceJobs.class);
		FileInputFormat.setInputPaths(job4,"Mutual_Friend_Stage2/"+OutputFileName);

		
		job4.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);

		// CG_MAPPER_HIDDEN
		job4.setMapperClass(Mutual_Friend_Mapper_Stage3.class);
		job4.getConfiguration().set("mapred.mapper.new-api", "true");

		// CG_MAPPER
		job4.getConfiguration().set("mapred.map.tasks", "3");
		job4.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		job4.setMapOutputValueClass(org.apache.hadoop.io.Text.class);

		// CG_PARTITIONER_HIDDEN
		job4.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);

		job4.setReducerClass(Mutual_Friend_Reducer_Stage3.class);
		job4.getConfiguration().set("mapred.reducer.new-api", "true");

		// CG_REDUCE
		job4.getConfiguration().set("mapred.reduce.tasks", "2");
		job4.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		job4.setOutputValueClass(org.apache.hadoop.io.Text.class);

		//job4.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		//conf4.setOutputKeyComparatorClass(DescendingIntComparable.class);

		
		// CG_OUTPUT_HIDDEN
		job4.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job4, new Path("Mutual_Friend_Stage3"));

		// FileInputFormat.setInputPaths(conf,
		// new Path(input + DATA_FILE_NAME));
		// FileOutputFormat.setOutputPath(conf, new Path(output));

		// JobClient.runJob(conf);

		job4.submit();
		job4.waitForCompletion(true);
		if(topFriendsList.size()!=0)
		{
		quickSort(topFriendsList, 0,topFriendsList.size() - 1);
		try
		 {
			Integer count = 0; 
			    fw = new FileWriter("Output/Mutual_Friends",false);
				BufferedWriter out = new BufferedWriter(fw);
				for(String tempString:topFriendsList) {
					count++;
					if(count == 5)
						break;
		            out.write(tempString);
		            out.newLine();
				} 
				out.close();
		 }
		 catch(Exception ex)
		 {
			 	System.out.println(ex);
		 }
		 finally
		 {
			  if (fw != null)
			  {
			   try { fw.close(); } 
			   catch (Exception e) { } 
			  }
			  fw=null;
		}
		}
		else
		{
		 	System.out.println("There are no mutual friends");
		}
 }
	public static void quickSort(ArrayList<String> finalList, int left, int right) {
		int i = left, j = right;
		String tmp;
		int pivot = (left + right) / 2;
		String pivotValue = finalList.get(pivot);
		String tempPivot[] = pivotValue.split("\\::");
		while (i <= j) {
			while (Double.parseDouble(finalList.get(i).toString().split("\\::")[1]) > Double
					.parseDouble(tempPivot[1]))
				i++;
			while (Double.parseDouble(finalList.get(j).toString().split("\\::")[1]) < Double
					.parseDouble(tempPivot[1]))
				j--;
			if (i <= j) {
				tmp = finalList.get(i);
				finalList.set(i, finalList.get(j));
				finalList.set(j, tmp);
				i++;
				j--;
			}
		}
		if (left < j)
			quickSort(finalList, left, j);
		if (i < right)
			quickSort(finalList, i, right);
	}
}