package com.atguigu.mapreducetest.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName：LogMapper
 * @Author: zcq
 * @Date: 2022/10/29 11:25
 * @Description:
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    /**
     * Map阶段的核心业务逻辑（将数据按行读取 然后直接输出）
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}
