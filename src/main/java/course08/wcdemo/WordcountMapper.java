package course08.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * keyin：在默认情况下是mr框架所读取到的一行文本的起始偏移量，long
 * 但是在hadoop中有自己的更加精简的实现序列化接口 所以不直接使用Long 而是使用LongWritable
 * <p>
 * valuein 默认情况下是mr框架所读取到的一行文本的内容String 同上 使用Text
 * <p>
 * keyout 是用户自定义逻辑处理完成之后输出数据中的key 在此处是单词 String 同上 使用Text
 * valueout 是用户自定义逻辑处理完成之后输出数据中的value 在此处是Integer 同上IntWritable
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * map阶段的业务逻辑就卸载自定义的map()方法中
     * maptask会对每一行输出数据调用一次我们定义的map方法
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将maptask传递给我们的文本内容先转换成String
        String line = value.toString();

        //根据空格将这一行切分为单词
        String[] words = line.split(" ");
        //将单词输出位<单词,1>
        for (String word : words) {
            //将单词作为key 把次数作为value 以便于后续的数据分发
            // 可以根据单词分发 以便于相同单词会分配到相同的reduece task中去
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
