package zfx.mapReduce.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * mapper 输出的key类型
 * mapper 输出的value类型
 * reduce 处理完后输出的key类型
 * reduce 处理完后输出的value类型
 */
public class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    /**
     * reduce 方法提供 reduce    task 进程来调用
     * shuffle 阶段将大量kv 聚合在一起，然后调用reduce 方法，
     *比如：<hello,1><hello,1>   <tom,1><tom,1><tom,1>
     *     传入参数： key为 一组kv中的key
     *               values为 kv中的所有value的迭代器
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable value:values){
            count+=value.get(); // 返回一个int
        }
        //输出这个单词的统计结果
        context.write(new Text(key),new IntWritable(count));
    }
}
