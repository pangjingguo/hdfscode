package com.atguigu.mapreducetest.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName：LogOutputFormat
 * @Author: zcq
 * @Date: 2022/10/29 11:33
 * @Description: 自定义OutputFormat需要继承Hadoop提供的OutputFormat
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {
    /**
     * 获取 RecordWriter 对象 负责写出数据
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        LogRecordWriter logRecordWriter = new LogRecordWriter(context);
        return logRecordWriter;
    }

}
