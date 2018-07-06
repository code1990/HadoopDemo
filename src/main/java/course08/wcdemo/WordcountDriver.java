package course08.wcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关的运行参数 指点jar包
 * 最后提交过yarn
 */
public class WordcountDriver {
    public static void main(String[] args) throws Exception {

        if (args == null || args.length == 0) {
            args = new String[2];
            args[0] = "hdfs://master:9000/wordcount/input/wordcount.txt";
            args[1] = "hdfs://master:9000/wordcount/output8";
        }

        Configuration conf = new Configuration();

        //设置的没有用
//        conf.set("HADOOP_USER_NAME","hadoop");
//        conf.set("dfs.permissions.enabled","false");
//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.hostname","mini1");

        Job job = Job.getInstance(conf);
        //指定本程序jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/reducer业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //指定mappper输出参数的key value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终要输出的数据的类型key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job的输入原始文件所在的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果所在的目录
        FileInputFormat.setInputPaths(job, new Path(args[1]));

        //将job中配置的相关参数以及job所用的java类型所在的jar包提交到yarn中去运行
        boolean res = job.waitForCompletion(true);
        System.out.println(res ? 0 : 1);


    }

}
