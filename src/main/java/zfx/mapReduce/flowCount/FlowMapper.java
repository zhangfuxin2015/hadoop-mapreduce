package zfx.mapReduce.flowCount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhangfuxin on 16/12/4.
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    /**
     * <起始偏移量，数据,手机号,bean>
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到一行文本内容
        String line=value.toString();
        //hadoop 的方法， 切分，没用 jdk的
        String[] fields=StringUtils.split(line," ");
        //抽取需要的字段
        String phone=fields[0];
        Long up=Long.parseLong(fields[1]);
        Long down=Long.parseLong(fields[2]);
        //输出 kv对  <电话号码，一个流量bean>
        // new FlowBean 最好不要 全new 写个变量去接收
        context.write(new Text(phone),new FlowBean(up,down));
    }
}
