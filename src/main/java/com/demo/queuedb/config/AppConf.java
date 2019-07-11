package com.demo.queuedb.config;

import com.alibaba.fastjson.JSONObject;
import com.demo.queuedb.bdb.BdbQueue;
import com.demo.queuedb.lmdb.LmdbQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * The type App conf.
 */
@Configuration
public class AppConf {

    /**
     * The lmdb path.
     */
    @Value("${file.server.lmdb.path}")
    private String lmdbPath;

    /**
     * The Lmdb size.
     */
    @Value("${file.server.lmdb.size}")
    private long lmdbSize;
    /**
     * The Bdb path.
     */
    @Value("${file.server.bdb.path}")
    private String bdbPath;


    /**
     * BDB文件队列
     *
     * @return the file queue
     * @throws IOException the io exception
     */
    @Bean
    public BdbQueue<JSONObject> bdbQueue() throws IOException {
        //确认存在文件夹
        Files.createDirectories(Paths.get(bdbPath));
        //新建一个BDB文件队列
        return new BdbQueue<>(bdbPath, "image", JSONObject.class);
    }

    /**
     * LMDB文件队列
     *
     * @return the lmdb queue
     * @throws IOException the io exception
     */
    @Bean
    public LmdbQueue<JSONObject> lmdbQueue() throws IOException {
        //确认存在文件夹
        Files.createDirectories(Paths.get(lmdbPath));
        //新建一个LMDB文件队列
        return new LmdbQueue<>(lmdbSize, 1, lmdbPath, 1, "image", JSONObject.class);
    }

}
