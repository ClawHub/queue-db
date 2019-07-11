package com.demo.queuedb.lmdb;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;


/**
 * LMDB客户端操作，一个环境，对应多个库
 *
 * @author lizhiming
 */
public class LmdbClient {

    /**
     * The Map size.
     */
    private long mapSize;

    /**
     * The Readers.
     */
    private int readers;

    /**
     * The Dbs.
     */
    private int dbs;

    /**
     * The Env path.
     */
    private String envPath;

    /**
     * The Env.
     */
    private Env<ByteBuffer> env;

    /**
     * Sets map size.
     *
     * @param mapSize the map size
     * @return the map size
     */
    public LmdbClient setMapSize(long mapSize) {
        this.mapSize = mapSize;
        return this;
    }

    /**
     * Sets readers.
     *
     * @param readers the readers
     * @return the readers
     */
    public LmdbClient setReaders(int readers) {
        this.readers = readers;
        return this;
    }

    /**
     * Sets dbs.
     *
     * @param dbs the dbs
     * @return the dbs
     */
    public LmdbClient setDbs(int dbs) {
        this.dbs = dbs;
        return this;
    }

    /**
     * Sets env path.
     *
     * @param envPath the env path
     * @return the env path
     */
    public LmdbClient setEnvPath(String envPath) {
        this.envPath = envPath;
        return this;
    }

    /**
     * Build lmdb client.
     *
     * @return the lmdb client
     */
    public LmdbClient build() {
        env = Env.create()
                .setMapSize(mapSize)
                .setMaxReaders(readers)
                .setMaxDbs(dbs)
                .open(new File(envPath), EnvFlags.MDB_FIXEDMAP, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_WRITEMAP);
        //提升性能
        System.setProperty(Env.DISABLE_CHECKS_PROP, Boolean.TRUE.toString());
        return this;
    }

    /**
     * Creat dbi dbi.
     *
     * @param dbName the db name
     * @return the dbi
     */
    public Dbi<ByteBuffer> creatDbi(String dbName) {
        if (env == null || env.isClosed()) {
            throw new Env.AlreadyClosedException();
        }
        return env.openDbi(dbName, DbiFlags.MDB_CREATE);
    }

    /**
     * Txn read txn.
     *
     * @return the txn
     */
    public Txn<ByteBuffer> txnRead() {
        if (env == null || env.isClosed()) {
            throw new Env.AlreadyClosedException();
        }
        return env.txnRead();
    }

    /**
     * Txn write txn.
     *
     * @return the txn
     */
    public Txn<ByteBuffer> txnWrite() {
        if (env == null || env.isClosed()) {
            throw new Env.AlreadyClosedException();
        }
        return env.txnWrite();
    }

    /**
     * Close.
     */
    public void close() {
        if (env != null && !env.isClosed()) {
            env.close();
        }
    }
}
