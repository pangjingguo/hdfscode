package com.atguigu.mapreducetest.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName：WordCountCombiner
 * @Author: zcq
 * @Date: 2022/10/29 10:45
 * @Description: 自定义Combiner需要继承Hadoop提供Reducer
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outv = new IntWritable();

    /**
     * Reduce阶段核心业务逻辑处理（相同的key一组数据调用一次reduce方法进行汇总）
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // 遍历vlaues 进行累加汇总
        for (IntWritable value : values) {
            sum += value.get();
        }
        outv.set(sum);
        // 将结果写出
        context.write(key, outv);
    }
}
