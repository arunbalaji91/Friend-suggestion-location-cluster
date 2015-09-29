import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KMeansMapper extends Mapper<Text, Text, Text, Text> {

	public ArrayList<String> getRandomSeeds() {
		ArrayList<String> finalSeeds=new ArrayList<>();	
		FileReader f1=null;
		try
		{
		    f1 = new FileReader("Input/Centroid.txt");
			BufferedReader br = new BufferedReader(f1);
			String line = br.readLine();
			int cnt=0;
			while (line != null)
			{
				String[] sp = line.split("::");
				finalSeeds.add(sp[0]);
				cnt++;
	            line = br.readLine();
			} 
			br.close();
		}
		catch (Exception e)
		{
			System.out.println("File not found. Please make sure the input file is present");
		}
		finally
		{
			  if (f1 != null)
			  {
			   try { f1.close(); } 
			   catch (Exception e) { } 
			  }
			  f1=null;
		}
		return finalSeeds;
	}
	static String[] splittingFunction(String inputString)
	{
		inputString = inputString.replaceAll("\t"," ");
		String tempString = inputString.toString().replaceAll("\\s\\s+", " ").trim();
		if(tempString != null)
		{
		String splitString[] = tempString.split(" ");
		return splitString;
		}
		else
			return null;
	}

	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException
	{
		// TODO: please implement your mapper code here
		ArrayList<String> seeds=getRandomSeeds();
		double min1, min2 = Double.MAX_VALUE;
		String nearest_center=seeds.get(0);
		if(seeds!=null)
		//Find the minimum center from a point
		for (String curr : seeds)
		{
			curr = curr.replaceAll("\t"," ");
			String []CurrSplit=splittingFunction(curr);
			String []ValueSplit=splittingFunction(value.toString());
			min1 = MapReduceJobs.calc_distance(Double.parseDouble(CurrSplit[2]),Double.parseDouble(CurrSplit[3]),
					Double.parseDouble(ValueSplit[1]),Double.parseDouble(ValueSplit[2]));
			
			if (Math.abs(min1) < Math.abs(min2)) 
			{
				nearest_center = curr;
				min2 = min1;
			}
		}
		context.write(new Text(nearest_center),new Text(key.toString()+" "+value.toString()));
	}
}
