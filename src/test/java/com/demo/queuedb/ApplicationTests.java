package com.demo.queuedb;

import com.alibaba.fastjson.JSONObject;
import com.demo.queuedb.bdb.BdbQueue;
import com.demo.queuedb.lmdb.LmdbQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

/**
 * The type Application tests.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationTests {

    /**
     * The Lmdb queue.
     */
    @Autowired
    private LmdbQueue<JSONObject> lmdbQueue;

    /**
     * The Bdb queue.
     */
    @Autowired
    private BdbQueue<JSONObject> bdbQueue;

    private JSONObject obj;

    /**
     * Before.
     *
     * @throws IOException the io exception
     */
    @Before
    public void before() throws IOException {
        obj = new JSONObject();
        //图片：110,592 字节
        obj.put("image", Base64.getEncoder().encode(Files.readAllBytes(Paths.get("C:\\Users\\lizhiming\\Desktop\\测试样本\\sfz.jpg"))));
    }

    @Test
    public void lmdb() {
        for (int i = 0; i < 100; i++) {
            lmdbQueue.offer(obj);
        }
        System.out.println(lmdbQueue.size());
        System.out.println(lmdbQueue.peek());
        System.out.println(lmdbQueue.size());
        System.out.println(lmdbQueue.poll());
        System.out.println(lmdbQueue.size());
    }

    @Test
    public void bdb() {
        for (int i = 0; i < 100; i++) {
            bdbQueue.offer(obj);
        }
        //同步写入文件协同
        bdbQueue.sync();

        System.out.println(bdbQueue.size());
        System.out.println(bdbQueue.peek());
        System.out.println(bdbQueue.peek());
        System.out.println(bdbQueue.size());
        System.out.println(bdbQueue.poll());
        System.out.println(bdbQueue.poll());
        System.out.println(bdbQueue.size());
        //同步写入文件协同
        bdbQueue.sync();
    }

    @Test
    public void test() {
        //--------------------------size----------------------
        System.out.println("lmdb size:" + lmdbQueue.size());
        System.out.println("bdb size:" + bdbQueue.size());
        //--------------------------bdb offer----------------------
        long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            bdbQueue.offer(obj);
        }
        //同步写入文件协同
        bdbQueue.sync();
        System.out.println("bdb offer 耗时：" + (System.nanoTime() - start) / 1000);
        //--------------------------lmdb offer----------------------
        start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            lmdbQueue.offer(obj);
        }
        System.out.println("lmdb offer 耗时：" + (System.nanoTime() - start) / 1000);
        //--------------------------bdb poll----------------------
        start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            bdbQueue.poll();
        }
        //同步写入文件协同
        bdbQueue.sync();
        System.out.println("bdb poll 耗时：" + (System.nanoTime() - start) / 1000);

        //--------------------------lmdb offer----------------------
        start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            lmdbQueue.poll();
        }
        System.out.println("lmdb poll 耗时：" + (System.nanoTime() - start) / 1000);

        //--------------------------size----------------------
        System.out.println("lmdb size:" + lmdbQueue.size());
        System.out.println("bdb size:" + bdbQueue.size());

        //不手动执行 bdbQueue.sync()方法，测试结果
        //lmdb size:0
        //bdb size:0
        //bdb offer 耗时：303738
        //lmdb offer 耗时：323650
        //bdb poll 耗时：446820
        //lmdb poll 耗时：42868
        //lmdb size:0
        //bdb size:0

        //手动执行 bdbQueue.sync()方法，测试结果
        //lmdb size:0
        //bdb size:0
        //bdb offer 耗时：6290361
        //lmdb offer 耗时：386631
        //bdb poll 耗时：432605
        //lmdb poll 耗时：67527
        //lmdb size:0
        //bdb size:0


        //总体来说LMDB作为队列性能比BDB略微好一点点。
    }

}
