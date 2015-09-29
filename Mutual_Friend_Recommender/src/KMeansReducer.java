import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.commons.collections.IteratorUtils;

public class KMeansReducer extends Reducer <Text, Text, Text, Text> {
	public static Integer randInt(Integer min, Integer max) {

	    Random rand = new Random();
	    Integer randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum;
	}
	 public static String GetCentralGeoCoordinate(
			 ArrayList<String> geoCoordinates)
		    {
		        if (geoCoordinates.size() == 1)
		        {
		            return geoCoordinates.get(0);
		        }

		        double x = 0;
		        double y = 0;
		        double z = 0;

		        for(String geoCoordinate: geoCoordinates)
		        {
		        	double latitude=Double.parseDouble(KMeansMapper.splittingFunction(geoCoordinate)[2]);
		        	double longitude=Double.parseDouble(KMeansMapper.splittingFunction(geoCoordinate)[3]);
		            x += Math.cos(latitude) * Math.cos(longitude);
		            y += Math.cos(latitude) * Math.sin(longitude);
		            z += Math.sin(latitude);
		        }

		        double total = geoCoordinates.size();

		        x = x / total;
		        y = y / total;
		        z = z / total;

		        double centralLongitude = Math.atan2(y, x);
		        double centralSquareRoot = Math.sqrt(x * x + y * y);
		        double centralLatitude = Math.atan2(z, centralSquareRoot); //randInt(1,100000).toString()
		        String newCentroid=randInt(1,100000).toString()+" "+ "0"+" "+Double.toString(centralLatitude * 180 / Math.PI)+" "+Double.toString(centralLongitude * 180 / Math.PI)+" "+"0";
		        return newCentroid;
		    }
	
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO: please implement your reducer here
		String points = "";
		ArrayList<String> coordinates=new ArrayList<>();
		for (Text value : values)
		{
			points = points + "::" + value;
			coordinates.add(value.toString());
		}
		String newCenter=GetCentralGeoCoordinate(coordinates);
		// Emit new center and point
		context.write(new Text(newCenter),new Text(points));
	}
}
