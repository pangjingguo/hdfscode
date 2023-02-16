package com.atguigu.mapreducetest.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName：FlowReducer
 * @Author: zcq
 * @Date: 2022/10/28 9:28
 * @Description:
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

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
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int totalUpFlow = 0;
        int totalDownFlow = 0;
//        int totalSumFlow = 0;
        // 循环遍历当前相同key对应的一组values 进行流量汇总
        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
//            totalSumFlow += value.getSumFlow();
        }

        // 封装输出结果
        outv.setUpFlow(totalUpFlow);
        outv.setDownFlow(totalDownFlow);
//        outv.setSumFlow(totalSumFlow);
        outv.setSumFlow();
        context.write(key, outv);
    }
}
