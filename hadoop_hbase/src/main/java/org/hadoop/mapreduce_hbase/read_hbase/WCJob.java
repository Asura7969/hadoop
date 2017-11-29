package org.hadoop.mapreduce_hbase.read_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


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
        conf.set("hbase.zookeeper.quorum","ndoe02,node03,node04");
        //
        try {
            Configuration config = HBaseConfiguration.create();
            Job job = new Job(config,"mapreduceReadHbase");
            job.setJarByClass(WCJob.class);
            Scan scan = new Scan();
            scan.setCaching(500);
            scan.setCacheBlocks(false); // don't set to true for MR jobs 不要为mapreduce程序设置true

            String tableName = "wordTable";
            TableMapReduceUtil.initTableMapperJob(tableName,scan,WCMapper.class,Text.class,IntWritable.class,job);

            job.setReducerClass(WCReudcer.class);

            Path outputPath = new Path("/user/output/wc");

            if(outputPath.getFileSystem(conf).exists(outputPath)){
                outputPath.getFileSystem(conf).delete(outputPath,true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

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
