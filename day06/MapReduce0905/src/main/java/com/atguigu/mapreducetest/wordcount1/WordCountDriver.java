package com.atguigu.mapreducetest.wordcount1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName：WordCountDriver
 * @Author: zcq
 * @Date: 2022/10/27 14:37
 * @Description: 一个job的驱动类
 * TODO 提交集群
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明一个配置对象
        Configuration conf = new Configuration();
        // 声明Job对象
        Job job = Job.getInstance(conf);
        // 指定当前job的主类的
        job.setJarByClass(WordCountDriver.class);
        // 指定当前Job的Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 指定Map阶段输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 指定MR最终输出结果的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        // 指定输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交任务
        job.waitForCompletion(true);
    }
}
