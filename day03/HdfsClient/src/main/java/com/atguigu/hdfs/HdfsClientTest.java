package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName：HdfsClientTest
 * @Author: zcq
 * @Date: 2022/10/4 11:23
 * @Description: HDFS的Java客户端操作
 * 面向客户端开发：
 * 1. 获取客户端连接对象
 * 2. 调用API完成功能
 * 3. 关闭资源
 */
public class HdfsClientTest {

    private FileSystem fileSystem;

    /**
     * 在执行Test之前先执行一次（做一些初始化）
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void init() throws IOException, InterruptedException {
        /**
         * Params:
         * uri – of the filesystem  连接HDFS的地址 hdfs://hadoop102:9820
         * conf – the configuration to use 临时添加的配置对象
         * user – to perform the get as  指定操作HDFS的用户名
         */
        URI uri = URI.create("hdfs://hadoop102:9820");
//        URI uri1 = new URI("hdfs://hadoop102:9820");
        // 参数优先级配置（代码 > 自定义配置 > 默认配置）
        Configuration conf = new Configuration();
        conf.set("dfs.replication","2");
        String user = "atguigu";
        // 1. 获取客户端连接对象
        fileSystem = FileSystem.get(uri, conf, user);
        System.out.println(fileSystem.getClass().getName());
    }

    /**
     * 在执行Test之后先执行一次（做一些收尾工作）
     */
    @After
    public void close() throws IOException {
        // 3. 关闭资源
        fileSystem.close();
    }



    /**
     * 上传功能
     */
    @Test
    public void testPut() throws URISyntaxException, IOException, InterruptedException {
     /*   *//**
         * Params:
         * uri – of the filesystem  连接HDFS的地址 hdfs://hadoop102:9820
         * conf – the configuration to use 临时添加的配置对象
         * user – to perform the get as  指定操作HDFS的用户名
         *//*
        URI uri = URI.create("hdfs://hadoop102:9820");
//        URI uri1 = new URI("hdfs://hadoop102:9820");
        Configuration conf = new Configuration();
        String user = "atguigu";
        // 1. 获取客户端连接对象
        FileSystem fileSystem = FileSystem.get(uri, conf, user);*/
        // 2. 调用API完成功能
        /**
         * Params:
         * delSrc – whether to delete the src
         * overwrite – whether to overwrite an existing file
         * src – path
         * dst – path
         */
        fileSystem.copyFromLocalFile(false, true,
                new Path("E:\\hdfs\\atguigu.txt"), new Path("/sanguo"));


    }


    /**
     *  下载功能
     */
    @Test
    public void testGet() throws IOException, InterruptedException {
        // 调用API完成功能
        /**
         * Params:
         * delSrc – whether to delete the src
         * src – path
         * dst – path
         * useRawLocalFileSystem – whether to use RawLocalFileSystem as local file system or not.
         */
        fileSystem.copyToLocalFile(false, new Path("/sanguo/guanyu.txt"), new Path("E:\\hdfs"), false);
    }
}
