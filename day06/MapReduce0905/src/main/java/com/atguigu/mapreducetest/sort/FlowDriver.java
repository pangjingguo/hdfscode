package com.atguigu.mapreducetest.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName：FlowDriver
 * @Author: zcq
 * @Date: 2022/10/28 9:28
 * @Description: 驱动类
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明配置对象
        Configuration conf = new Configuration();
        // 声明Job对象
        Job job = Job.getInstance(conf);
        // 指定当前job的Mapper和Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 指定当前job的Map阶段输出key和value
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        // 指定当前job最终输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 指定自定义的比较器对象
        job.setSortComparatorClass(MyWritableComparator.class);
        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("E:\\input\\phone_data"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\output\\phone_data_out2"));
        // 提交job
        job.waitForCompletion(true);
    }
}
