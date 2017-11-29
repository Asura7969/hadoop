package org.hadoop.mapreduce_hbase.write_hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by gongwenzhou on 2017/11/29.
 *
 * 读取hdfs中的文件
 */
public class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //
        String[] split = StringUtils.split(value.toString(), '\t');

        for (String s : split) {
            context.write(new Text(s),new IntWritable(1));
        }
    }
}
