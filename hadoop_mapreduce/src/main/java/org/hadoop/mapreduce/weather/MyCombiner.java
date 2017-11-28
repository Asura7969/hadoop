package org.hadoop.mapreduce.weather;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 每年的每个月每一天如果存在多条温度记录。进行聚合处理。每天留下气温最高的那条。其他不要。
 * @author root
 *
 */
public class MyCombiner extends Reducer<MyKey, Text, MyKey, Text>{

	protected void reduce(MyKey key, Iterable<Text> iter, Context context)
			throws IOException, InterruptedException {
		for(Text t: iter){
			context.write(key, t);
			break;
		}
	}
}
