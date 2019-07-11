package com.demo.queuedb.bdb;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;

/**
 * BDB数据库环境
 */
public class BdbEnvironment extends Environment {

    /**
     * classCatalog
     */
    private StoredClassCatalog classCatalog;

    /**
     * classCatalogDB
     */
    private Database classCatalogDB;

    /**
     * Constructor
     *
     * @param envHome   数据库环境目录
     * @param envConfig config options 数据库环境配置
     * @throws DatabaseException <br>
     */
    public BdbEnvironment(File envHome, EnvironmentConfig envConfig) {
        super(envHome, envConfig);
    }

    @Override
    public synchronized void close() {
        if (classCatalogDB != null) {
            classCatalogDB.close();
        }
        super.close();
    }
}
