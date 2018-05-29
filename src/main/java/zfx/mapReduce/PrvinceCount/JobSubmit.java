package zfx.mapReduce.PrvinceCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by zhangfuxin on 16/12/4.
 */
public class JobSubmit {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(JobSubmit.class);
        job.setMapperClass(ProvinceMapper.class);
        job.setReducerClass(ProvinceReduceMapper.class);
        // 如果map和 reduce 输出类型一致，那么可以不用设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //自定义 shuffle  来给reduce里面   ，替换掉系统默认的hashPartitioner
        job.setPartitionerClass(ProvincePartitioner.class);
        //设置reduce Task 数量  这个是 partitioner 可能给的reduceTask
        job.setNumReduceTasks(3);
        FileInputFormat.setInputPaths(job,"hdfs://zhangfuxindeMacBook-Pro.local:9000/phone/srcdata/");
        FileOutputFormat.setOutputPath(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/phone/test8/"));
        boolean resut = job.waitForCompletion(true);
        if(resut==true){
            System.out.println("成功");
        }else{
            System.out.println("失败");
        }
    }
}
