package hdfs;

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

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("");
        Configuration entries = new Configuration();
        String user = "atguigu";
        fileSystem = FileSystem.get(uri, entries, user);
    }

    @After
    public void clos() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testCopyFromLocal() throws IOException, InterruptedException, URISyntaxException {


        fileSystem.copyFromLocalFile(false, true, new Path(""), new Path(""));


    }

    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {

        fileSystem.copyToLocalFile(new Path(""),new Path(""));

    }
}
