package org.hadoop.mapreduce.weather;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<MyKey, Text, Text, NullWritable>{

	public HashMap<Integer, Text> dayData =new HashMap<Integer, Text>();
	
	protected void reduce(MyKey key, Iterable<Text> iter,Context context)
			throws IOException, InterruptedException {
		
		dayData.clear();
		
		int i=0;//计数器，取前三个
		for(Text t:iter	){
			Text v =dayData.get(key.getDay());
			if(v==null){
				dayData.put(key.getDay(), t);
				i++;
				if(i>=4){
					break;
				}else{
					context.write(t, NullWritable.get());
				}
			}else{
				continue;
			}
		}
	}
}
