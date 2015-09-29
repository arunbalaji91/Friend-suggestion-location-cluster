import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
public class Mutual_Friend_Mapper extends Mapper<Text, Text, Text, Text>  {
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException
	{
				
				//Going through the users in the nearest centroid found earlier to check if the element is present in the cluster 
				FileReader f3 = new FileReader("job_output" + (MapReduceJobs.iteration-1) + "/"+ MapReduceJobs.OutputFileName);
				BufferedReader br3 = new BufferedReader(f3);
				String line = br3.readLine();
				boolean flag=false;
				while (line != null)
				{
					String records[]=line.split("::");
					if(records[0].equals(MapReduceJobs.centroid) || flag == true )//may need to change the records 0
					{
						for(int i=1;i<records.length;i++)
						{
							if(MapReduceJobs.userId==Integer.parseInt(KMeansMapper.splittingFunction(records[i])[0]))
							{
								flag=true; //If in case the chosen is not present in the cluster
								int cnt=0;
								boolean key_present=false;
								boolean value_present=false;
								String dup=null;
								for(int j=1;j<records.length;j++)
								{
									key_present=Integer.parseInt(key.toString())==Integer.parseInt(KMeansMapper.splittingFunction(records[j])[0]);
									value_present=Integer.parseInt(value.toString())==Integer.parseInt(KMeansMapper.splittingFunction(records[j])[0]);
								if(key_present||value_present)
								{
									if(cnt==0 && key_present)
									{
										dup=key.toString();
									   cnt++;
									}
									else if(cnt==0 && value_present)
									{
										dup=value.toString();
										cnt++;
									}
									else
									{
										if(cnt==1 && !dup.equals(key) && !dup.equals(value))
											cnt++;
									}
								}
										if(cnt==2)
										{	
												context.write(key,value);
												break;
										}
								}//for loop ends
							}	
							if(flag==true) break;
						}
					}//if loop ends	 
					if(flag==true) break;
					 line = br3.readLine();
				}
				
				br3.close();
				f3.close();
	}
	

}
