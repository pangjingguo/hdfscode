package com.atguigu.mapreducetest.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @ClassName：MapJoinMapper
 * @Author: zcq
 * @Date: 2022/10/29 15:24
 * @Description:
 */
public class MapJoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 声明一个配置对象
        Configuration conf = new Configuration();
        // 声明Job对象
        Job job = Job.getInstance(conf);
        // 指定当前Job的Mapper
        job.setMapperClass(MapJoinMapper.class);
        // 指定Map阶段输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 指定MR最终输出结果的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        // 指定ReduceTask的数量为0
        job.setNumReduceTasks(0);
        // 指定要缓存的文件路径
        job.addCacheFile(URI.create("file:///E:/input/cachefile/pd.txt"));
        // 指定输入和输出路径
        FileInputFormat.addInputPath(job, new Path("E:\\input\\mapjoin"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\output\\mapjoin_out2"));
        // 提交任务
        job.waitForCompletion(true);
    }
}
