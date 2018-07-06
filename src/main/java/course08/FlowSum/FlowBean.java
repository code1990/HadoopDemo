package course08.FlowSum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 流量统计相关需求
 * 1、对流量日志中的用户统计总上、下行流量
 * 技术点： 自定义javaBean用来在mapreduce中充当value
 * 注意： javaBean要实现Writable接口，实现两个方法
 * <p>
 * 2、统计流量且按照流量大小倒序排序
 * 技术点：这种需求，用一个mapreduce -job 不好实现，需要两个mapreduce -job
 * 第一个job负责流量统计，跟上题相同
 * 第二个job读入第一个job的输出，然后做排序
 * 要将flowBean作为map的key输出，这样mapreduce就会自动排序
 * 此时，flowBean要实现接口WritableComparable
 * 要实现其中的compareTo()方法，方法中，我们可以定义倒序比较的逻辑
 * <p>
 * 3、统计流量且按照手机号的归属地，将结果数据输出到不同的省份文件中
 * 自定义partition后，要根据自定义partitioner的逻辑设置相应数量的reduce task
 */
public class FlowBean implements Writable {

    private long upFlow;//上行流量
    private long dFlow;//下行流量
    private long sumFlow;//流量消耗总量

    //反序列化时候 需要反射调用空参构造器 所以需要显示的调用一个

    public FlowBean() {
    }

    public FlowBean(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow + dFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + dFlow + "\t" + sumFlow;
    }

    /**
     * 序列化方法
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(dFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * 注意：反序列化方法 的顺序与序列化的顺序保持一致
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        dFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }
}
