package com.atguigu.mapreducetest.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName：FlowMapper
 * @Author: zcq
 * @Date: 2022/10/28 9:28
 * @Description: Map阶段的核心的业务处理
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outk = new Text();
    private FlowBean outv = new FlowBean();

    /**
     * map阶段的核心业务逻辑（按行读取，然后获取其中手机号和流量信息，组合成kv结构输出即可!!!）
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取当前行数据
        String lineData = value.toString();
        // 切割  5 	18271575951	192.168.100.2	www.atguigu.com	1527	2106	200
        String[] datas = lineData.split("\t");
        // 获取手机号 封装输出的key
        String phoneNum = datas[1];
        outk.set(phoneNum);
        // 获取流量信息 封装输出value
        outv.setUpFlow(Integer.parseInt(datas[datas.length - 3]));
        outv.setDownFlow(Integer.parseInt(datas[datas.length - 2]));
        outv.setSumFlow();

        // 输出结果即可
        context.write(outk, outv);

    }
}
