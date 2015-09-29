import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Mutual_Friend_Reducer_Stage2  extends Reducer <Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO: please implement your reducer here
		// Emit new center and point
		boolean flag=false;
		Integer cnt=0;
		for (Text value : values) 
		{
			if(Integer.parseInt(value.toString())==-1)
			{   
				flag=true;
				break;
			}
			if(Integer.parseInt(value.toString())==1)
				cnt=cnt+1;	
		} 
		if(flag==false)
		context.write(key,new Text(cnt.toString()));
	}
}
