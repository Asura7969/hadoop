package org.hadoop.mapreduce.fof;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Created by root on 2017/10/22.
 */

/**
 * Configuration conf = new Configuration(true);
 * true/false   读不读配置文件
 * false：conf.set("fs.defaultFS","hdfs://node01:9000");
 *
 * 1、服务器集群运行
 *      a）打jar包
 *          1）hadoop jar方式，需要jar发送到集群
 *          2）直接运行client端代码
 *              job.setJar("jar-path");
 *              conf.set("mapreduce.app-submission.cross-platform","true");
 *  2、直接本地模拟运行
 *      （1）Configuration conf = new Configuration(true);
 *          conf.set("mapreduce.framework.name","local");
 *          conf.set("mapreduce.app-submission.cross-platform","true");
 *          无需job.setJar("jar-path");
 *      （2）Configuration conf = new Configuration(false);
 *          conf.set("fs.defaultFS","hdfs://node01:9000");
 *          conf.set("mapreduce.app-submission.cross-platform","true");
 */
public class FofjobRun {

    public static void main(String[] args) throws IOException {

        try {
            Configuration conf = new Configuration(true);
            //conf.set("mapreduce.framework.name","local");
            //win
            conf.set("mapreduce.app-submission.cross-platform","true");
            Job job = Job.getInstance(conf);

            job.setJarByClass(FofjobRun.class);
            job.setJar("E:\\gwz_ideaWorkSpace\\hadoop\\out\\artifacts\\hadoop_mapreduce_jar\\hadoop_mapreduce_jar.jar");
//            job.setJar("F:\\gitworkspace\\first_hadoop_demp\\out\\artifacts\\first_hadoop_demp_jar\\first_hadoop_demp.jar");
            //input

            //map
            /**
             * map端代码：获取该次mapper task的数据片段
             *              FileSplit fs = (FileSplit)context.getInputSplit();
             *              boolean contains = fs.getPath().getName().contains("part-r-00003");
             *
             *
             *
             * 读取hdfs上的文件，缓存在内存中，在map端可以获取该缓存文件
             *job.addCacheFile(new Path("/user/tfidf/output/weibo1/part-r-00003").toUri());
             * 在map端实现setup方法
             *URI[] files = context.getCacheFiles();    获取上面的缓存文件
             */

            job.setMapperClass(Fmapper.class);
            //设置map端的key和value
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //partition:默认hashpartition
            //reduce端并行
            //解决数据倾斜

            //sort 1：用户设置   2：key

            //combiner

            //reduce
            job.setReducerClass(Freduce.class);

            //input output path
            Path input = new Path("/user/root/input");
            FileInputFormat.addInputPath(job,input);

            Path output = new Path("/output/friend2");
            if(output.getFileSystem(conf).exists(output)){
                output.getFileSystem(conf).delete(output,true);
            }
            FileOutputFormat.setOutputPath(job, output);
            //提交job
            job.waitForCompletion(true);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }



}
