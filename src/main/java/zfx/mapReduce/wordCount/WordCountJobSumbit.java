package zfx.mapReduce.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 目的是统计 单词出现的次数
 */
public class WordCountJobSumbit   {
    public static void main(String[] args) throws Exception {
        //构造一个配置参数封装对象
        Configuration conf=new Configuration();
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","zhangfuxindeMacBook-Pro.local:9000");
        Job job=Job.getInstance(conf);
        //指定job所在的jar包
        job.setJarByClass(WordCountJobSumbit.class);
        //设置job 所用的mapper逻辑类
        job.setMapperClass(WordCountMapper.class);
        //设置job 所用的reduce 逻辑类
        job.setReducerClass(WordCountReduce.class);
        //设置map阶段输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置reduce 阶段输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定一下处理的文本数据 存放的位置。
        FileInputFormat.setInputPaths(job,"hdfs://zhangfuxindeMacBook-Pro.local:9000/wordcount/srcdata/");
        //设置处理输出路径，存放位置
        FileOutputFormat.setOutputPath(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/wordcount/outPut55533/"));
        //提交到hadoop中运行
        job.waitForCompletion(true);
        System.out.println("成功");

        //zip -d your jar META-INF/LICENSE   当不能 create file的时候在mac 下跑
    }
}
