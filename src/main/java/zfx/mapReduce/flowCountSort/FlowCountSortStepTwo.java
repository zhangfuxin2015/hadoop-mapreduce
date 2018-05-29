package zfx.mapReduce.flowCountSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import zfx.mapReduce.flowCount.*;

import java.io.IOException;

/**
 * Created by zhangfuxin on 16/12/7.
 */
public class FlowCountSortStepTwo {
    /**
     *这是流量汇总中第二个步骤，处理的数据是汇总步骤的输出结果
     * 这是统计的第二个步骤， 偏移量， 数据， 流量的上传量，手机号
     */
    public  static class  FlowCountSortStepTwoMapper extends Mapper<LongWritable,Text,FlowBean,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line=value.toString();
            String[] fileds = line.split("\t");
            String phone =fileds[0];
            long up=Long.parseLong(fileds[1]);
            long down=Long.parseLong(fileds[2]);
            //把流量信息作为key，一遍排序的结果符合我们的要求
            context.write(new FlowBean(up,down),new Text(phone));
        }
    }
    public  static class  FlowCountSortStepTwoReducer extends Reducer<FlowBean,Text,Text,FlowBean>{
        /**
         *不需要做汇总。
         */
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(),key);
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        //告诉客户端job 的jar包在哪里
        job.setJarByClass(FlowCountSortStepTwo.class);
        job.setMapperClass(FlowCountSortStepTwoMapper.class);
        job.setReducerClass(FlowCountSortStepTwoReducer.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job,"hdfs://zhangfuxindeMacBook-Pro.local:9000/phone/test14/");
        FileOutputFormat.setOutputPath(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/phone/test16/"));
        job.waitForCompletion(true);
        System.out.println("成功了");
    }

}
