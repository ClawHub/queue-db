package com.demo.queuedb.lmdb;

import com.demo.queuedb.util.ByteBufferUtil;
import com.demo.queuedb.util.KryoPoolFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The type Lmdb queue.
 *
 * @param <E> the type parameter
 */
public class LmdbQueue<E extends Serializable> extends AbstractQueue<E> implements Serializable {
    /**
     * The Logger.
     */
    private Logger logger = LoggerFactory.getLogger(LmdbQueue.class);
    /**
     * The First index.
     */
    private volatile long firstIndex;
    /**
     * The Last index.
     */
    private AtomicLong lastIndex;
    /**
     * The Lmdb client.
     */
    private LmdbClient lmdbClient;
    /**
     * The Dbi.
     */
    private Dbi<ByteBuffer> dbi;
    /**
     * The Entries.
     */
    private AtomicLong entries;
    /**
     * The Base class.
     */
    private Class<E> baseClass;

    /**
     * Instantiates a new Lmdb queue.
     *
     * @param mapSize   the map size
     * @param dbs       the dbs
     * @param envPath   the env path
     * @param readers   the readers
     * @param dbName    the db name
     * @param baseClass the base class
     */
    public LmdbQueue(long mapSize, int dbs, String envPath, int readers, String dbName, Class<E> baseClass) {
        //初始化LMDB环境
        lmdbClient = new LmdbClient().setDbs(dbs).setEnvPath(envPath).setMapSize(mapSize).setReaders(readers).build();
        //创建DB
        dbi = lmdbClient.creatDbi(dbName);
        //基础类类型，用于序列化与反序列化
        this.baseClass = baseClass;
        //初始化指针
        initIndex();
    }

    /**
     * 初始化指针
     */
    private void initIndex() {
        //获取读事务
        Txn<ByteBuffer> txnRead = lmdbClient.txnRead();
        //获取 firstIndex
        ByteBuffer byteBuffer = dbi.get(txnRead, ByteBufferUtil.stringToByteBuffer("index_queue_first_index"));
        if (byteBuffer != null) {
            //获取lmdb中的值，如果没有则默认0
            firstIndex = byteBuffer.getLong();
            //除去index_queue_first_index所占用的1个位置
            entries = new AtomicLong(dbi.stat(txnRead).entries - 1);
        } else {
            //系统启动后，获取lmdb中数据量多少
            entries = new AtomicLong(dbi.stat(txnRead).entries);
        }

        //提交事务
        txnRead.commit();

        //尾指针
        lastIndex = new AtomicLong(entries.longValue() + firstIndex);
    }

    /**
     * Peek e.
     *
     * @return the e
     */
    @Override
    public synchronized E peek() {
        ByteBuffer byteBuffer;
        try (Txn<ByteBuffer> txnRead = lmdbClient.txnRead()) {
            //获取byteBuffer
            byteBuffer = dbi.get(txnRead, ByteBufferUtil.longToByteBuffer(firstIndex));
            txnRead.commit();
            if (byteBuffer == null) {
                return null;
            }
        }
        //byteBuffer
        return readObject(byteBuffer);
    }

    /**
     * Read object e.
     *
     * @param byteBuffer the byte buffer
     * @return the e
     */
    private E readObject(ByteBuffer byteBuffer) {
        //byteBuffer
        if (byteBuffer == null) {
            return null;
        }
        //反序列化
        try (Input in = new ByteBufferInput(byteBuffer)) {
            //获取序列化器
            Kryo kryo = KryoPoolFactory.INSTANCE.getPool().borrow();
            //执行反序列化
            E result = kryo.readObjectOrNull(in, baseClass);
            //释放序列化器
            KryoPoolFactory.INSTANCE.getPool().release(kryo);
            return result;
        }
    }


    /**
     * Poll e.
     *
     * @return the e
     */
    @Override
    public synchronized E poll() {
        ByteBuffer byteBuffer;
        try (Txn<ByteBuffer> txnWrite = lmdbClient.txnWrite()) {
            //获取byteBuffer
            byteBuffer = dbi.get(txnWrite, ByteBufferUtil.longToByteBuffer(firstIndex));
            if (byteBuffer == null) {
                txnWrite.commit();
                return null;
            }
            boolean isDel = dbi.delete(txnWrite, ByteBufferUtil.longToByteBuffer(firstIndex));
            firstIndex += 1;
            dbi.put(txnWrite, ByteBufferUtil.stringToByteBuffer("index_queue_first_index"), ByteBufferUtil.longToByteBuffer(firstIndex));
            //数量-1
            entries.decrementAndGet();
            txnWrite.commit();
        }
        //byteBuffer
        return readObject(byteBuffer);
    }

    /**
     * Offer boolean.
     *
     * @param item the item
     * @return the boolean
     */
    @Override
    public boolean offer(E item) {
        //获取一个指针位
        long last = lastIndex.getAndIncrement();
        //序列化
        try (Output out = new Output(0, 20971520)) {
            //获取序列化器
            Kryo kryo = KryoPoolFactory.INSTANCE.getPool().borrow();
            //序列化
            kryo.writeClassAndObject(out, item);
            //释放序列化器
            KryoPoolFactory.INSTANCE.getPool().release(kryo);
            //入库
            dbi.put(ByteBufferUtil.longToByteBuffer(last), ByteBufferUtil.bytesToByteBuffer(out.getBuffer()));
            //数量+1
            entries.incrementAndGet();
            return true;
        } catch (Exception e) {
            logger.error("lmdb offer fail." + e);
            return false;
        }
    }


    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("lmdb not support");
    }

    /**
     * Size int.
     *
     * @return the int
     */
    @Override
    public int size() {
        return entries.intValue();
    }

    /**
     * Close.
     */
    public void close() {
        if (dbi != null) {
            dbi.close();
        }
        lmdbClient.close();
    }
}
