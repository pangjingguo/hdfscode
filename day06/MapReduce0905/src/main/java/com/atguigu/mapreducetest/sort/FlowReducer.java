package com.atguigu.mapreducetest.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName：FlowReducer
 * @Author: zcq
 * @Date: 2022/10/28 9:28
 * @Description:
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    private FlowBean outv = new FlowBean();

    /**
     * reduce阶段的核心业务逻辑（根据相同的手机号进行流量汇总 然后将结果输出即可）
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
