package org.hadoop.mapreduce_hbase.write_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;


/**
 * Created by gongwenzhou on 2017/11/29.
 *
 * 通过mapreduce向hbase中 写 数据
 *
 * 在windows上运行mapreduce程序，需要修改源码
 * 这里直接复制 NativeIO 类(包名、类名与源码相同)
 */
public class WCJob {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node02:8020");
        //
        conf.set("hbase.zookeeper.quorum","node02,node03,node04");

        try {
            Job job = Job.getInstance();
            job.setJarByClass(WCJob.class);

            job.setMapperClass(WCMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //
            String targetTable = "WC";

            TableMapReduceUtil.initTableReducerJob(targetTable,WCReudcer.class,job);

            /**
             * inputPath的路径直接写/user/root/input/wc会出现 Input path does not exist:
             *
             * 具体原因未知,但是加上 core-site.xml中的fs.defaultFS就OK了
             */
            Path inputPath = new Path("hdfs://mycluster/user/root/input/wc");
            FileInputFormat.addInputPath(job,inputPath);

            job.setNumReduceTasks(1);

            boolean flag = job.waitForCompletion(true);
            if(flag){
                System.out.println("运行成功！");

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
