package com.atguigu.mapreducetest.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @ClassName：MapJoinDriver
 * @Author: zcq
 * @Date: 2022/10/29 15:24
 * @Description: MapJoin的案例
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final HashMap<String,String> pdMap = new HashMap<>();
    private final Text outk = new Text();

    /**
     * 读取Job中的缓存文件
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取Job中的缓存文件的路径
        URI[] cacheFiles = context.getCacheFiles();
        URI cacheFile = cacheFiles[0];
        // 获取问价系统对象
        FileSystem fs = FileSystem.get(context.getConfiguration());
        // 获取一个输入流
        FSDataInputStream inputStream = fs.open(new Path(cacheFile));
        // 数据读取写入内存中的map中
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, "utf-8"));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            // 切割 01	小米
            String[] pdDatas = line.split("\t");
            pdMap.put(pdDatas[0],pdDatas[1]);
        }

        // 关闭资源
        IOUtils.closeStream(fs);
        IOUtils.closeStream(bufferedReader);

    }

    /**
     * Map阶段的核心业务（读取order文件数据 获取到pid 根据pid到缓存中获取对应的pname 封装输出结果）
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
        // 切割  1001	01	1
        String[] datas = lineData.split("\t");
        // 获取pid
        String pid = datas[1];
        // 到缓存的容器中获取 对应的pname
        String pname = pdMap.get(pid);
        // 封装输出结果
        String result = datas[0] + "\t" + pname + "\t" + datas[2];
        // 将结果输出
        outk.set(result);
        context.write(outk, NullWritable.get());
    }
}
