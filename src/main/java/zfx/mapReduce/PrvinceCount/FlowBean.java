package zfx.mapReduce.PrvinceCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *自定义数据类型，要在hadoop 中传递，需要实现hadoop的序列化
 */
public class FlowBean implements Writable {
    private long upFlow;
    private long downFlow;

    @Override
    public String toString() {

        return  upFlow+"::"+downFlow;
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
    }

    /**
     *因为反射机制的存在，需要定义一个无参构造函数
     */
    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    /**
     *  序列化方法，将传输的数据序列化成字节流
     */
    public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(upFlow);
            dataOutput.writeLong(downFlow);
    }

    /**
     *反序列化，从字节流中恢复各个字段  怎嘛添加的怎嘛读字段的顺序
     */

    public void readFields(DataInput dataInput) throws IOException {
        upFlow=dataInput.readLong();
        downFlow=dataInput.readLong();
    }
}
