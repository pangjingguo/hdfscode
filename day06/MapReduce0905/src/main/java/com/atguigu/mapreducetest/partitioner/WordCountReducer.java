package com.atguigu.mapreducetest.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName：WordCountReducer
 * @Author: zcq
 * @Date: 2022/10/27 14:36
 * @Description: 自定义的Reducer需要继承Hadoop提供的Reducer类
 *  * 1. 输入数据的key和value的类型：
 *  *      KEYIN,  Map阶段输出key的类型
 *  *      VALUEIN, Map阶段输出value的类型
 *  * 2. 输出数据的key和value的类型：
 *  *     KEYOUT, 当前输出的一个单词  Text
 *  *     VALUEOUT 当前单词出现的总数  IntWritable
 *  * 3. 重写父类的reduce() 方法，实现自定义的业务逻辑
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
