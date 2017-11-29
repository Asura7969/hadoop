package org.hadoop.mapreduce_hbase.write_hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by gongwenzhou on 2017/11/29.
 *
 * 写入hbase中
 */
public class WCReudcer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        //rowKey:取单词名字
        Put put = new Put(key.getBytes());
        //"cf":列族   "wordCount":列名  sum:值(单词对应的个数)
        put.addColumn("cf".getBytes(),"wordCount".getBytes(),(sum + "").getBytes());

        context.write(null,put);
    }
}
