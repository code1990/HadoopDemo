package course08.FlowSum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
public class FlowCount {

    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //将一行内容转换成String
            String line = value.toString();
            //切分字段
            String[] fields = line.split("\t");
            //取出手机号码
            String phoneNbr = fields[1];
            //取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long dFlow = Long.parseLong(fields[fields.length - 2]);

            context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));
        }
    }

    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        //<1380000000,bean1><13800000001,bean2>
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_dFlow = 0;

            //遍历所有的bean 把其中的上行流量下行流量分别累加
            for (FlowBean bean : values) {
                sum_upFlow += bean.getUpFlow();
                sum_dFlow += bean.getdFlow();
            }

            FlowBean resultBean = new FlowBean(sum_upFlow, sum_dFlow);
            context.write(key, resultBean);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //添加有关的集群配置
//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.hostname","mini1");
        Job job = Job.getInstance(conf);

        //设置集群jar包的所在位置
//        job.setJar("/home/hadoop/wc.jar");
        //指定本程序的Jar包所在的本地路径
        job.setJarByClass(FlowCount.class);

        //指定本业务的job要使用的mapper、reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定我们自定义的数据分区器
//        job.setPartitionerClass(ProvincePartitioner.class);
//        //同时指定相应“分区”数量的reducetask
//        job.setNumReduceTasks(5);

        //指定mapper输出数据的key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终输出的数据的key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数以及job所用的java类所在的jar包 提交到yarn去运行
//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.out.println(res ? 0 : 1);

    }


}
