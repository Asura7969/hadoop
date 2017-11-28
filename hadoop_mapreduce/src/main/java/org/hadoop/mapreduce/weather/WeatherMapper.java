package org.hadoop.mapreduce.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<Text, Text, MyKey, Text>{

	public static SimpleDateFormat SDF =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String date =key.toString();
		
		try {
			Calendar c =Calendar.getInstance();
			c.setTime(SDF.parse(date));
			int year =c.get(Calendar.YEAR);
			int month =c.get(Calendar.MONTH);
			int day =c.get(Calendar.DAY_OF_MONTH);
			double hot =Double.parseDouble(value.toString().substring(0, value.toString().length()-1));
			
			MyKey k =new MyKey(year, month, hot,day);
			context.write(k, new Text(key.toString() +"\t" +value.toString()));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}
}
