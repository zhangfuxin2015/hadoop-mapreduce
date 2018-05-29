package zfx.mapReduce.JoinOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import zfx.mapReduce.friends.CommonFriendsStepTwo;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by zhangfuxin on 16/12/10.
 */
public class ReduceSideJoin {
  public  static class ReduceSideJoinMapper extends Mapper<LongWritable,Text,Text,JoinBean>{
      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fileds=line.split(",");
        // 需要判断，需要处理的数据来自哪个文件。
          FileSplit inputSplit = (FileSplit) context.getInputSplit();
          String  orderId="";
          String  itemId="";
          String  amount="";
          String  itemType="";
          String flag="";
          String  fileName=inputSplit.getPath().getName();
          // 如果来自 订单详情表
          if(fileName.equals("detail.txt")){
              orderId=fileds[0];
              itemId=fileds[1];
              amount=fileds[2];
              flag="1";
          }else{
              //如果来自订单类型表
              itemId=fileds[0];
              itemType=fileds[1];
              flag="2";
          }
          //<11,xxx><11,xxx>  <12,xxx>
          context.write(new Text(itemId),new JoinBean(orderId,itemId,amount,itemType,flag));
      }
  }
  public  static  class  ReduceSideJoinReduce extends Reducer<Text,JoinBean,NullWritable,JoinBean>{
      //<11,xxx>
      @Override
      protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
          ArrayList<JoinBean> joinBeanList = new ArrayList<JoinBean>();
          String type=null;
        for(JoinBean joinBean:values){
            // 2为商品类型表
            if(joinBean.getFlag().equals("2")){
                type=joinBean.getItemType();
            }else{
                joinBeanList.add(new JoinBean(joinBean.getOrderId(),
                        joinBean.getItemId(),joinBean.getAmount(),
                        joinBean.getItemType(),joinBean.getFlag()));

            }
        }
          for(JoinBean j:joinBeanList){
              j.setItemType(type);
              context.write(NullWritable.get(),j);
          }
      }
  }

    public static void main(String[] args) throws Exception {
        Job job= Job.getInstance(new Configuration());
        job.setJarByClass(ReduceSideJoinMapper.class);
        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setReducerClass(ReduceSideJoinReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(JoinBean.class);
        FileInputFormat.setInputPaths(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/order/data/"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://zhangfuxindeMacBook-Pro.local:9000/order/result4"));
        job.waitForCompletion(true);
        System.out.println("成功第一步");
    }
}
