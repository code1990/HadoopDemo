package course08.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * keyin valuein对应mapper输出的keyout valueout类型对应
 * keyout valueout是自定义reduce逻辑处理结果的输出数据类型
 * keyout是单词
 * valueout是次数
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * <a,1><b,1>
     *
     * @param key     入参key是一组相同的单词key-value的key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
