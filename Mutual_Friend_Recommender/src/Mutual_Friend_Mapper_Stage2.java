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
public class Mutual_Friend_Mapper_Stage2 extends Mapper<Text, Text, Text, Text>  {
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException
	{
        String[] friends = value.toString().split(",");
        for(int i=0;i<friends.length;i++)
        {
        	String usr_id_pair = Integer.parseInt(key.toString()) < Integer.parseInt(friends[i]) ? key+","+friends[i] : friends[i]+","+ key;
            context.write(new Text(usr_id_pair), new Text("-1"));
        	for(int j=i+1;j<friends.length;j++)
        	{        		
        		String cart_prod = Integer.parseInt(friends[i]) < Integer.parseInt(friends[j]) ? friends[i]+","+friends[j] : friends[j]+","+friends[i];
        		context.write(new Text(cart_prod), new Text("1"));
        	}
        }
	}
	

}
