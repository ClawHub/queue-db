package com.demo.queuedb.bdb;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.EnvironmentConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The type Bdb queue.
 *
 * @param <E> the type parameter
 */
public class BdbQueue<E extends Serializable> extends AbstractQueue<E> implements Serializable {

    /**
     * logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(BdbQueue.class);

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -4175431971478491701L;

    /**
     * 数据库环境
     */
    private transient BdbEnvironment dbEnv;

    /**
     * 数据库，用于保存值，支持队列序列化
     */
    private transient Database queueDb;

    /**
     * 持久化Map,Key为指针位置,Value为值,无需序列化
     */
    private transient StoredSortedMap<Long, E> queueMap;

    /**
     * 数据库所在位置
     */
    private transient String dbDir;

    /**
     * 数据库名
     */
    private transient String dbName;

    /**
     * 尾部指针
     */
    private AtomicLong tailIndex;

    /**
     * 当前获取的值
     */
    private transient volatile E peekItem = null;

    /**
     * 构造函数,传入BDB数据库
     *
     * @param db         db
     * @param valueClass valueClass
     */
    public BdbQueue(Database db, Class<E> valueClass) {
        this.queueDb = db;
        this.dbName = db.getDatabaseName();
        //绑定数据库
        bindDatabase(queueDb, valueClass);
        //初始化指针
        initIndex();
    }

    /**
     * 构造函数,传入BDB数据库位置和名字,自己创建数据库
     *
     * @param dbDir      <br>
     * @param dbName     <br>
     * @param valueClass <br>
     */
    public BdbQueue(String dbDir, String dbName, Class<E> valueClass) {
        this.dbDir = dbDir;
        this.dbName = dbName;
        createAndBindDatabase(valueClass);
        initIndex();

    }

    /**
     * 初始化指针
     */
    private void initIndex() {
        if (queueMap.isEmpty()) {
            tailIndex = new AtomicLong(0);
        } else {
            tailIndex = new AtomicLong(queueMap.lastKey() + 1);
        }
    }


    /**
     * 绑定数据库
     *
     * @param db         db
     * @param valueClass valueClass
     */
    private void bindDatabase(Database db, Class<E> valueClass) {
        queueDb = db;

        //使用Kryo序列化
        EntryBinding<E> valueBinding = new KryoBinding<>(valueClass);
        queueMap = new StoredSortedMap<>(db, TupleBinding.getPrimitiveBinding(Long.class), valueBinding, true);
    }

    /**
     * 创建以及绑定数据库
     *
     * @param valueClass valueClass
     */
    private void createAndBindDatabase(Class<E> valueClass) {
        Database db;
        try {
            //创建数据库
            db = createDb();
            //绑定数据库
            bindDatabase(db, valueClass);
        } catch (Exception e) {
            LOGGER.error("create db failed", e);
            throw e;
        }

    }

    /**
     * 创建数据库
     *
     * @return the database
     */
    private Database createDb() {
        // 数据库位置
        File envFile = new File(dbDir);
        // 数据库环境配置
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        dbEnv = new BdbEnvironment(envFile, envConfig);
        // 数据库配置
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);

        dbConfig.setTransactional(false);
        dbConfig.setDeferredWrite(true);
        return dbEnv.openDatabase(null, dbName, dbConfig);
    }

    /**
     * 值遍历器
     */
    @Override
    public Iterator<E> iterator() {
        return queueMap.values().iterator();
    }


    /**
     * 大小
     */
    @Override
    public int size() {
        try {
            return Math.max(0,
                    (int) (tailIndex.get()
                            - queueMap.firstKey()));
        } catch (IllegalStateException | NoSuchElementException | NullPointerException ise) {
            return 0;
        }
    }

    /**
     * 插入值
     */
    @Override
    public boolean offer(E e) {
        long targetIndex = tailIndex.getAndIncrement();
        queueMap.put(targetIndex, e);
        return true;
    }

    /**
     * 从头部获取值，将删除的值保存在peekItem
     */
    @Override
    public synchronized E peek() {
        if (peekItem == null) {
            if (queueMap.isEmpty()) {
                return null;
            }
            peekItem = queueMap.remove(queueMap.firstKey());
        }
        return peekItem;
    }

    /**
     * 从头部获取值,并删除当前值
     */
    @Override
    public synchronized E poll() {
        E head = peek();
        peekItem = null;
        return head;
    }

    @Override
    public boolean isEmpty() {
        if (peekItem != null) {
            return false;
        }
        try {
            return queueMap.isEmpty();
        } catch (IllegalStateException de) {
            return true;
        }
    }

    /**
     * 关闭所用的BDB数据库但不关闭数据库环境。
     */
    private void close() {
        try {
            if (queueDb != null) {
                //同步写入
                queueDb.sync();
                queueDb.close();
            }

        } catch (Exception e) {
            LOGGER.error("close BDB failed", e);
        }
    }

    /**
     * 关闭所用的BDB数据库 同时关闭数据库环境。
     */
    public void closeDbAndEnv() {
        try {
            //闭所用的BDB数据库但不关闭数据库环境。
            close();
            if (dbEnv != null && queueDb != null) {
                //关闭数据库环境
                dbEnv.close();
            }
        } catch (DatabaseNotFoundException e) {
            LOGGER.error("close BDB and base failed", e);
        }
    }

    /**
     * 清理,会清空数据库,并且删掉数据库所在目录,慎用.如果想保留数据,请调用close()
     */
    @Override
    public void clear() {
        try {
            //关闭所用的BDB数据库但不关闭数据库环境。
            close();
            if (dbEnv != null && queueDb != null) {
                //清空数据
                if (dbName == null) {
                    dbEnv.removeDatabase(null, queueDb.getDatabaseName());
                } else {
                    dbEnv.removeDatabase(null, dbName);
                }
                //关闭环境
                dbEnv.close();
            }
        } catch (Exception e) {
            LOGGER.error("clear BDB failed", e);
        } finally {
            try {
                //物理删除
                if (this.dbDir != null) {
                    FileUtils.deleteDirectory(new File(this.dbDir));
                }
            } catch (IOException e) {
                LOGGER.error("del DBD direct failed", e);
            }
        }
    }

    /**
     * Sync.
     */
    public void sync() {
        queueDb.sync();
    }

}
