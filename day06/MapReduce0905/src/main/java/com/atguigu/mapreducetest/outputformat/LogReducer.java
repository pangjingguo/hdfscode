package com.atguigu.mapreducetest.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName：LogReducer
 * @Author: zcq
 * @Date: 2022/10/29 11:25
 * @Description:
 */
public class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    /**
     * Reducer阶段的核心业务（遍历values将数据输出）
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key, value);
        }
    }

    public void test(){
        String i="1";
        Integer A=2;
        int o;
        Integer B;

        int integer = Integer.valueOf(i);

        Integer i1 = Integer.parseInt(i);

        int i2 = Integer.valueOf(i).intValue();
    }
}
