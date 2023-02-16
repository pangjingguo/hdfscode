package com.atguigu.mapreducetest.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @ClassName：ReduceJoinReducer
 * @Author: zcq
 * @Date: 2022/10/29 14:13
 * @Description:
 */
public class ReduceJoinReducer extends Reducer<Text, OrderPd, OrderPd, NullWritable> {

    private ArrayList<OrderPd> orderPds = new ArrayList<>();
    private OrderPd op = new OrderPd();

    /**
     * Reduce阶段的核心业务逻辑（将当前有关联的一组数据 根据不同的数据来源分离， 再关联数据 最后输出）
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<OrderPd> values, Context context) throws IOException, InterruptedException {
        // 遍历当前相同key的一组values
        for (OrderPd orderPd : values) {
            // 区分数据来源
            if(orderPd.getTitle().equals("order.txt")){
                try {
                    OrderPd thisOp = new OrderPd();
                    BeanUtils.copyProperties(thisOp, orderPd);
                    orderPds.add(thisOp);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else {
                // 来源pd文件
                try {
                    BeanUtils.copyProperties(op, orderPd);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        // 通过 op 来获取pname 然后给 orderPds中的OrderPd中的pname赋值
        for (OrderPd orderPd : orderPds) {
            orderPd.setPname(op.getPname());
            // 输出结果
            context.write(orderPd, NullWritable.get());
        }
        // 清空 orderPds
        orderPds.clear();
    }
}
