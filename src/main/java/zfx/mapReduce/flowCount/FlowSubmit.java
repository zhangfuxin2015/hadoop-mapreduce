package zfx.mapReduce.flowCount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *目的是统计统计 每个电话号码， 网上下载流量， 上传流量
 */
public class FlowSubmit {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        //告诉客户端job 的jar包在哪里
        job.setJarByClass(FlowSubmit.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(flowReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job,"hdfs://zhangfuxindeMacBook-Pro.local:9000/phone/srcdata/");
        FileOutputFormat.setOutputPath(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/phone/test14/"));
        job.waitForCompletion(true);
        System.out.println("成功了");
    }
}
