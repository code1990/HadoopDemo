package course08.FlowSum;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * key value对应于map输出的key value的类型
 * 自定义partition后，要根据自定义partitioner的逻辑设置相应数量的reduce task
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    public static HashMap<String, Integer> provinceDict = new HashMap<String, Integer>();

    static {
        provinceDict.put("136", 0);
        provinceDict.put("137", 1);
        provinceDict.put("138", 2);
        provinceDict.put("139", 3);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prefix = text.toString().substring(0, 3);
        Integer provinceId = provinceDict.get(prefix);

        return provinceId == null ? 4 : provinceId;
    }
}
