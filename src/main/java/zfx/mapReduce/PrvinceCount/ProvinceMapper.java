package zfx.mapReduce.PrvinceCount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhangfuxin on 16/12/4.
 */
public class ProvinceMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    private Text text=new Text();
    private FlowBean flowBean=new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fileds=StringUtils.split(line," ");
        String phone=fileds[0];
        long upFlow=Long.parseLong(fileds[1]);
        long downFlow=Long.parseLong(fileds[2]);
        text.set(phone);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpFlow(upFlow);
        context.write(text,flowBean);
    }
}
