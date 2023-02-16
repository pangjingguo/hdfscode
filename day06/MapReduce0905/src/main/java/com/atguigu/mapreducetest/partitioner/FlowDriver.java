package com.atguigu.mapreducetest.partitioner;

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
 * TODO 实现自定义分区案例
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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 指定当前job最终输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 设置分区数量
        job.setNumReduceTasks(5);
        job.setPartitionerClass(FlowPartitioner.class);
        // 指定输入输出路径
        FileInputFormat.addInputPath(job, new Path("F:\\大数据文件\\Hadoop\\02.资料\\07_测试数据\\phone_data"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\大数据文件\\Hadoop\\02.资料\\07_测试数据\\output\\wcoutput"));
        // 提交job
        job.waitForCompletion(true);
    }
}
