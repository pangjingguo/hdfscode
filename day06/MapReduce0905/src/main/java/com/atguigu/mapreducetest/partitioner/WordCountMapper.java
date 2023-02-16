package com.atguigu.mapreducetest.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName：WordCountMapper
 * @Author: zcq
 * @Date: 2022/10/27 14:36
 * @Description: 自定义的Mapper需要继承Hadoop提供的Mapper类
 * 1. 输入数据的key和value的类型：
 *      KEYIN, 待处理一行数据的偏移量 LongWritable
 *      VALUEIN, 当前一行数据  Text
 * 2. 输出数据的key和value的类型：
 *     KEYOUT, 当前输出的一个单词  Text
 *     VALUEOUT 给当前一个单词打标记1  IntWritable
 * 3. 重写父类的map() 方法，实现自定义的业务逻辑
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);

    /**
     * map阶段的核心的业务逻辑（读取一行数据 进行切割 将切割后的每一个单词打标记1 然后输出即可）
     * @param key  当前一行数据的偏移量
     * @param value  当前一行数据
     * @param context 上下文对象 （程序的流程化过程中充当承上启下的作用）
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取当前行数据
        String lineData = value.toString();
        // 切割
        String[] datas = lineData.split(" ");
        // 遍历datas给每一个单词进行打标记 输出
        for (String data : datas) {
            outk.set(data);
            context.write(outk,outv);
        }
    }
}
