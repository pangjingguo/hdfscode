package com.atguigu.mapreducetest.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName：FlowPartitioner
 * @Author: zcq
 * @Date: 2022/10/28 15:58
 * @Description: 自定义的分区器 需要继承 Hadoop提供的分区器Partitioner
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 自定义分区规则
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     * 136 --> 0
     * 137 --> 1
     * 138 --> 2
     * 139 --> 3
     * other --> 4
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int partitions;
        // 获取手机号
        String phNum = text.toString();
        if(phNum.startsWith("136")){
            partitions = 0;
        }else if(phNum.startsWith("137")){
            partitions = 1;
        }else if(phNum.startsWith("138")){
            partitions = 2;
        }else if(phNum.startsWith("139")){
            partitions = 3;
        }else {
            partitions = 4;
        }
        return partitions;
    }
}
