package zfx.mapReduce.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper<中的参数代表>
 * 输入数据 kv对中的key 的数据类型 在map阶段为 偏移
 * 输入数据 kv对中的value的数据类型  txt里面的txt
 * 输出数据  kv数据的 key的数据类型
 * 输出数据的 kv 数据的 value的数据类型
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    //map 方法提供maptask进程来调用的，maptask是每读取一行文本，调用一次map方法
    //传递的参数  一行的起始偏移量， 文本数据，
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到文本
        String line=value.toString();
        //将一行文本切分为单词
        String word[]=line.split(" ");
        //输出单词  <单词，1>
        for (String w:word){
            context.write(new Text(w),new IntWritable(1));

        }
    }
}
