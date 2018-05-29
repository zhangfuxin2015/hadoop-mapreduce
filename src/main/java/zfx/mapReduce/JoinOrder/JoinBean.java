package zfx.mapReduce.JoinOrder;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhangfuxin on 16/12/10.
 */
public class JoinBean implements Writable {
    private String  orderId;
    private String  itemId;
    private String  amount;
    private String  itemType;
    private String flag;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(itemId);
        dataOutput.writeUTF(amount);
        dataOutput.writeUTF(itemType);
        dataOutput.writeUTF(flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId=dataInput.readUTF();
        this.itemId=dataInput.readUTF();
        this.amount=dataInput.readUTF();
        this.itemType=dataInput.readUTF();
        this.flag=dataInput.readUTF();
    }

    public JoinBean(String orderId, String itemId, String amount, String itemType,String flag) {
        this.orderId = orderId;
        this.itemId = itemId;
        this.amount = amount;
        this.itemType = itemType;
        this.flag=flag;
    }
    public JoinBean(){}
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getItemType() {
        return itemType;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    @Override
    public String toString() {
        return this.orderId+"\t"+this.itemId+"\t"+this.itemType+"\t"+amount;
    }
}
