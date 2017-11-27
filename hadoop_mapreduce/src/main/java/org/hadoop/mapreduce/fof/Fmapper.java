package org.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by root on 2017/10/22.
 */
public class Fmapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text mKey = new Text();
    IntWritable mValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * 获取该次mapper task的数据片段
         *      FileSplit fs = (FileSplit)context.getInputSplit();
         *      boolean contains = fs.getPath().getName().contains("part-r-00003");
         */


        String[] strs = StringUtils.split(value.toString(),' ');
        String user = strs[0];

        for (int i = 0; i < strs.length - 1; i++) {
            //直接关系
            mKey.set(Fof.getName(user,strs[i]));
            mValue.set(0);
            context.write(mKey,mValue);
            String nuser = strs[i+1];
            for (int j = i+1; j < strs.length-1; j++) {
                //间接关系
                mKey.set(Fof.getName(nuser,strs[j]));
                mValue.set(1);
                context.write(mKey,mValue);
            }
        }
    }

    public static class Fof{
        public static String getName(String n1,String n2){
            if(n1.compareTo(n2)>0){
                return n2 + ":" + n1;
            }
            return n1 + ":" + n2;
        }
    }
}
