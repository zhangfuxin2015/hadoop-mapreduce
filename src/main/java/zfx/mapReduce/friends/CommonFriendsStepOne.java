package zfx.mapReduce.friends;

import jdk.nashorn.internal.scripts.JO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by zhangfuxin on 16/12/10.
 */
public class CommonFriendsStepOne {
    public  static  class  commonFriendsStepOneMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //A:B,C,D,F,E,O  输出  <B,A>   <C,A>  <D,A> <F,A> <E,A> <O,A>
            //B:A,C,E,K      输出  <A,B>   <C,B> <E,B> <K,B>
            String line=value.toString();
            String[] split=line.split(":");
            String[] friends=split[1].split(",");
            for(String f:friends){
                context.write(new Text(f),new Text(split[0]));
            }
        }
    }
    public  static  class  commonFriendsStepOneReduce extends Reducer<Text,Text,Text,Text>{
        //<B,A><B,E><B,F><B,J>  输入
        //输出<B,  A,E,F,J>   输出 每个人  都有哪些是 好友。
        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for (Text text:persons){
                sb.append(text.toString()).append(",");
            }
            context.write(friend,new Text(sb.toString()));
        }

    }

    public static void main(String[] args) throws Exception {
        Job job= Job.getInstance(new Configuration());
        job.setJarByClass(CommonFriendsStepOne.class);
        job.setMapperClass(commonFriendsStepOneMapper.class);
        job.setReducerClass(commonFriendsStepOneReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/friends/data/"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/friends/result"));
        job.waitForCompletion(true);
        System.out.println("成功第一步");
    }
}
