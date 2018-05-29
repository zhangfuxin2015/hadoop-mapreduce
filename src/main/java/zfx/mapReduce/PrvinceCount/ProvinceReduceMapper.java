package zfx.mapReduce.PrvinceCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zhangfuxin on 16/12/4.
 */
public class ProvinceReduceMapper extends Reducer <Text,FlowBean,Text,FlowBean>{
    private FlowBean flowBean=new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upCount=0;
        long downCount=0;
        for(FlowBean flowBean:values){
            upCount+=flowBean.getUpFlow();
            downCount+=flowBean.getDownFlow();
        }
        flowBean.setDownFlow(downCount);
        flowBean.setUpFlow(upCount);
        // hadoop 默认打印 POJO tostring方法，所以需要重新写
        context.write(key,flowBean);
    }
}
