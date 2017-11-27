package org.hadoop.mapreduce.friend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * Created by root on 2017/11/26.
 *
 * 好友推荐
 *      1、从社交数据中找到所有的FOF（user1:user2）
 *      2、去掉那些存在直接好友关系的FOF
 *      3、统计这些FOF出现的次数
 *      4、根据这些FOF出现的次数降序排序
 *      5、根据降序之后结果，给某个用推荐好友
 *
 * 思路:
 *      1、推荐者与被推荐者一定有一个或者多个相同的好友
 *      2、全局去查询好友列表中两两关系
 *      3、去除直接好友关系
 *      4、统计两两关系出现的次数
 * API:
 *      map:按好友列表输出两两关系
 *      reduce:sum两两关系
 *      再生成一个MR
 *      生成详细的报表
 */


/**
 * Configuration conf = new Configuration(true);
 * true/false   读不读配置文件
 * false：  conf.set("fs.defaultFS","hdfs://node01:9000");
 * true:    conf.set("mapreduce.framework.name","local");
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
public class RunJob {

    public static void main(String[] args) {
        Configuration conf = new Configuration(true);
//        conf.set("fs.defaultFS","hdfs://node01:9000");
        conf.set("mapreduce.framework.name","local");
        conf.set("mapreduce.app-submission.cross-platform","true");

//        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        Job job;
        try {
            job = Job.getInstance();
            job.setJobName("friend_1");
            job.setJarByClass(RunJob.class);
            job.setJar("F:\\gitworkspace\\hadoop_project\\out\\artifacts\\mapreduce_friend\\mapreduce_friend.jar");

            job.setMapperClass(MapperFriend1.class);
            job.setReducerClass(ReducerFriend1.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setCombinerClass(MyCombiner.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);

            FileInputFormat.addInputPath(job,new Path("/user/root/input"));

            Path outdir = new Path("/output/friend");
            if(outdir.getFileSystem(conf).exists(outdir)){
                outdir.getFileSystem(conf).delete(outdir,true);
            }
            FileOutputFormat.setOutputPath(job,outdir);
            //执行第一个mapreduce
            boolean jobSuccess = job.waitForCompletion(true);

            if(jobSuccess){
                System.out.println("第一个job执行成功！");
                job = Job.getInstance();
                job.setJobName("friend_2");
                //job的入口类
                job.setJarByClass(RunJob.class);

                job.setMapperClass(MapperFriend2.class);
                job.setReducerClass(ReducerFriend2.class);

                job.setMapOutputKeyClass(User.class);
                job.setMapOutputValueClass(Text.class);

                job.setGroupingComparatorClass(MyGroup.class);

                //KeyValueTextInputFormat 把第一个隔开符的左边为key，右边为value
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                FileInputFormat.addInputPath(job, new Path("/output/friend"));

                //输出结果数据目录不能存在，job执行时自动创建的。如果在执行时目录已经存在，则job执行失败。
                outdir =new Path("/output/friend2");
                if(outdir.getFileSystem(conf).exists(outdir)){
                    outdir.getFileSystem(conf).delete(outdir, true);
                }
                FileOutputFormat.setOutputPath(job,outdir);

                jobSuccess= job.waitForCompletion(true);
                if(jobSuccess){
                    System.out.println("第二个job执行成功！");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 输出直接好友和间接好友
     */
    static class MapperFriend1 extends Mapper<Text, Text, Text, IntWritable>{

        //定义输出的key
        Text k = new Text();
        //定义输出的value,是直接好友还是间接好友
        IntWritable v1 = new IntWritable(1);
        IntWritable v2 = new IntWritable(2);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String user = key.toString();
            String[] friends = StringUtils.split(value.toString(), '\t');
            String fof = null;
            for (int i = 0; i < friends.length; i++) {
                String friend1 = friends[i];
                fof = user.compareTo(friend1) > 0 ? user + ":" + friend1 : friend1 + ":" + user;
                k.set(fof);
                context.write(k,v2);//输出直接好友

                for (int j = i+1; j < friends.length; j++) {
                    String friend2 = friends[j];
                    if(friend1.compareTo(friend2) > 0){
                        fof = friend1 + ":" + friend2;
                    }else{
                        fof = friend2 + ":" + friend1;
                    }

                    k.set(fof);
                    context.write(k,v1);//输出间接好友
                }
            }
        }
    }

    static class ReducerFriend1 extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    static class MyCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            boolean flag = true;
            for (IntWritable value : values) {
                if(value.get() == 2){//直接好友,直接跳出
                    flag = false;
                    break;
                }else{
                    //是间接好友,value相加
                    sum = sum + value.get();
                }
            }
            if(flag){
                context.write(key,new IntWritable(sum));
            }
        }
    }

    static class MyGroup extends WritableComparator{
        public MyGroup(){
            super(User.class,true);
        }

        @Override
        public int compare(Object a, Object b) {
            User u1 = (User) a;
            User u2 = (User) b;
            return u1.getUser().compareTo(u2.getUser());
        }
    }

    static class MapperFriend2 extends Mapper<Text,Text,User,Text>{

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String f1 = key.toString().split(" : ")[0];
            String f2 = key.toString().split(" : ")[1];
            int count = Integer.valueOf(value.toString());

            User u1 = new User();
            u1.setUser(f1);
            u1.setOther(f2);
            u1.setCount(count);
            context.write(u1,new Text(f2 + ":" + count));

            User u2 = new User();
            u2.setUser(f2);
            u2.setOther(f1);
            u2.setCount(count);
            context.write(u1,new Text(f1 + ":" + count));
        }
    }

    static class ReducerFriend2 extends Reducer<User,Text,Text,Text>{

        StringBuilder sb = new StringBuilder();

        @Override
        protected void reduce(User key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                sb.append(value.toString()).append(",");
            }
            context.write(new Text(key.getUser()),new Text(sb.substring(0,sb.length() - 1)));
        }
    }

}
