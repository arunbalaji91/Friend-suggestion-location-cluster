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
public class Mutual_Friend_Mapper_Stage3 extends Mapper<Text, Text, Text, Text>  {
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException
	{
        String[] friend_pair = key.toString().split(",");
        if(Integer.parseInt(friend_pair[0])==Integer.parseInt(MapReduceJobs.userId.toString()) || Integer.parseInt(friend_pair[1])==Integer.parseInt(MapReduceJobs.userId.toString()))
        {
        	String input_user=Integer.parseInt(friend_pair[0])==Integer.parseInt(MapReduceJobs.userId.toString()) ?friend_pair[0]:friend_pair[1];
        	String mutual_friend=Integer.parseInt(friend_pair[0])==Integer.parseInt(MapReduceJobs.userId.toString()) ?friend_pair[1]:friend_pair[0];
        	MapReduceJobs.topFriendsList.add(mutual_friend+"::"+value.toString());
        	//context.write(new Text(value), new Text(mutual_friend));
        }
        	//        for(int i=0;i<friends.length;i++)
//        {
//        	String usr_id_pair = Integer.parseInt(key.toString()) < Integer.parseInt(friends[i]) ? key+","+friends[i] : friends[i]+","+ key;
//            context.write(new Text(usr_id_pair), new Text("-1"));
//        	for(int j=i+1;j<friends.length;j++)
//        	{        		
//        		String cart_prod = Integer.parseInt(friends[i]) < Integer.parseInt(friends[j]) ? friends[i]+","+friends[j] : friends[j]+","+friends[i];
//        		context.write(new Text(usr_id_pair), new Text("1"));
//        	}
//        }
	}
	

}
