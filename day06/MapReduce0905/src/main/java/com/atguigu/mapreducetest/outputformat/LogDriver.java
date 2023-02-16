package com.atguigu.mapreducetest.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName：LogDriver
 * @Author: zcq
 * @Date: 2022/10/29 11:25
 * @Description:
 */
public class LogDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明一个配置对象
        Configuration conf = new Configuration();
        // 声明Job对象
        Job job = Job.getInstance(conf);
        // 指定当前Job的Mapper和Reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        // 指定Map阶段输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 指定MR最终输出结果的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        // 指定自定义的OutputFormat
        job.setOutputFormatClass(LogOutputFormat.class);
        // 指定输入和输出路径
        FileInputFormat.addInputPath(job, new Path("E:\\input\\log"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\output\\log_out2"));
        // 提交任务
        job.waitForCompletion(true);
    }
}
