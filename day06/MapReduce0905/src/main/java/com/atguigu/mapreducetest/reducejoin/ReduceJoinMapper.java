package com.atguigu.mapreducetest.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName：ReduceJoinMapper
 * @Author: zcq
 * @Date: 2022/10/29 14:13
 * @Description:
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, OrderPd> {

    private Text outk = new Text();
    private OrderPd outv = new OrderPd();
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    /**
     * map阶段的核心业务逻辑（将两个文件的数据按行读取，进行数据整合并区分数据来源 最终输出）
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
        // 切割
        String[] datas = lineData.split("\t");
        // 判断数据来源（利用文件名称区别不同数据）
        if(fileName.endsWith("order.txt")){
            // 封装输出的key 和 value  1001	01	1
            outk.set(datas[1]);
            outv.setOrderId(Integer.parseInt(datas[0]));
            outv.setPid(Integer.parseInt(datas[1]));
            outv.setAmount(Integer.parseInt(datas[2]));
            outv.setPname("");
            outv.setTitle(fileName);
        }else {

            // 封装输出的key 和 value   01	小米
            outk.set(datas[0]);
            outv.setOrderId(0);
            outv.setPid(Integer.parseInt(datas[0]));
            outv.setAmount(0);
            outv.setPname(datas[1]);
            outv.setTitle(fileName);
        }

        context.write(outk, outv);
    }
}
