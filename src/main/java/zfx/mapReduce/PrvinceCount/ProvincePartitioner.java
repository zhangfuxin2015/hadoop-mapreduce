package zfx.mapReduce.PrvinceCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by zhangfuxin on 16/12/4.
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    private  static HashMap<String,Integer> provinces=new HashMap<String, Integer>();
    static {
        //模拟加载数据库，到本地内存
        provinces.put("123",1);
        provinces.put("333",2);
    }
    public int getPartition(Text text, FlowBean flowBean, int reduceTasks) {
        //拿到key -- 手机号前N位，然后查询外部的字典。
        String prefix = text.toString().substring(0, 3);
        //从字典中  查询归属地分区号
        Integer provincesNumber=provinces.get(prefix);
        if(provincesNumber==null){
            return 3;
        }
        return provincesNumber;
    }
}
