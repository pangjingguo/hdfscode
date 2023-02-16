package com.atguigu.mapreducetest.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @ClassName：LogRecordWriter
 * @Author: zcq
 * @Date: 2022/10/29 11:39
 * @Description:
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    // 定义输出路径
    private String atguiguPath = "F:\\大数据文件\\Hadoop\\02.资料\\07_测试数据\\output\\atguigu.log";
    private String otherPath = "F:\\大数据文件\\Hadoop\\02.资料\\07_测试数据\\output\\other.log";
    private FileSystem fs;
    private  FSDataOutputStream atguiguOut;
    private  FSDataOutputStream otherOut;

    // 构造器中用于传递Context对象
    public LogRecordWriter(TaskAttemptContext context) throws IOException {
        // 获取配置对象
        Configuration conf = context.getConfiguration();
        // 获取文件系统对象
        fs = FileSystem.get(conf);
        // 声明一个输出流对象
        atguiguOut = fs.create(new Path(atguiguPath));
        otherOut = fs.create(new Path(otherPath));

    }

    /**
     * 写数据的核心逻辑
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 获取当前数据
        String data = key.toString();
        if(data.contains("atguigu")){
            atguiguOut.writeBytes(data + "\n");
        }else {
            otherOut.writeBytes(data + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fs);
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
