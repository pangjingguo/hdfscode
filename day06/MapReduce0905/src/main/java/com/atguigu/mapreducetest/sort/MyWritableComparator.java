package com.atguigu.mapreducetest.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName：MyWritableComparator
 * @Author: zcq
 * @Date: 2022/10/29 9:44
 * @Description: 自定义比较器对象
 */
public class MyWritableComparator extends WritableComparator {

    // 无参构造器用于指定比较器对象是谁的比较器对象
    public MyWritableComparator(){
      super(FlowBean.class, true);
    }

    // 重写compare方法
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBean abean = (FlowBean) a;
        FlowBean bbean = (FlowBean) b;
        return abean.getSumFlow().compareTo(bbean.getSumFlow());
    }
}