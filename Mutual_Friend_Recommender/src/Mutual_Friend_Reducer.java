import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Mutual_Friend_Reducer  extends Reducer <Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO: please implement your reducer here
		// Emit new center and point
		String temp = new String();
		for (Text value : values) 
		{
			temp=temp+value.toString()+",";
		} 
		context.write(key,new Text(temp));
	}
}
