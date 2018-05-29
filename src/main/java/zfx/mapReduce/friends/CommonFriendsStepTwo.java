package zfx.mapReduce.friends;

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
import java.util.Arrays;

/**
 * Created by zhangfuxin on 16/12/10.
 */
public class CommonFriendsStepTwo {
    public  static  class  commonFriendsStepTowMapper extends Mapper<LongWritable,Text,Text,Text>{
        // 输入  A	B,F,K,G,H,O,I,D,C,
        // 输出 <B-F,A><B-K,A>
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] split=line.split("\t");
            String[] friends=split[1].split(",");
            //对用户列表 排序，以免出现 GF  和FG 不同的组合
            Arrays.sort(friends);
            //对数组，做俩俩组合拼接
            for(int i=0;i<friends.length-1;i++){
                for(int j=i+1;j<friends.length;j++){
                    context.write(new Text(friends[i]+"-"+friends[j]),new Text(split[0]));
                }
            }
        }
    }
    public  static  class  commonFriendsStepTwoReduce extends Reducer<Text,Text,Text,Text>{
        //输入<A-E,B>  <A-E,C>
        @Override
        protected void reduce(Text pair, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for(Text text:friends){
                sb.append(text).append(" ");
            }
            // 输出 <A-E B C>
            context.write(pair,new Text(sb.toString()));
        }

    }

    public static void main(String[] args) throws Exception {
        Job job= Job.getInstance(new Configuration());
        job.setJarByClass(CommonFriendsStepTwo.class);
        job.setMapperClass(commonFriendsStepTowMapper.class);
        job.setReducerClass(commonFriendsStepTwoReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/friends/result/"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/friends/result3"));
        job.waitForCompletion(true);
        System.out.println("成功第一步");
    }
}
