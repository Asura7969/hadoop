package org.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 2017/10/22.
 */
public class Freduce extends Reducer<Text,IntWritable,Text,Text> {

    Text rValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //相同的key为一组
        int sum = 0;
        boolean flag = true;
        for (IntWritable val : values) {
            if(val.get()==0){
                flag = false;
                break;
            }

            sum +=val.get();

        }
        if(flag){
            rValue.set(sum+"");
            context.write(key,rValue);
        }
    }
}
