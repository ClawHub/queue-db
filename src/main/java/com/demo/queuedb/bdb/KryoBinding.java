package com.demo.queuedb.bdb;

import com.demo.queuedb.util.KryoPoolFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

/**
 * The type Kryo binding.
 *
 * @param <K> the type parameter
 */
public class KryoBinding<K> implements EntryBinding<K> {
    /**
     * The Base class.
     */
    private Class<K> baseClass;
    /**
     * The Kryo.
     */
    private Kryo kryo;

    /**
     * Instantiates a new Kryo binding.
     * 因为只会初始化一次，所以获取到kryo之后，不释放
     *
     * @param baseClass the base class
     */
    public KryoBinding(Class<K> baseClass) {
        this.baseClass = baseClass;
        //获取kryo
        kryo = KryoPoolFactory.INSTANCE.getPool().borrow();
        //注册结构
        kryo.register(baseClass);
    }

    /**
     * 反序列化
     *
     * @param entry entry
     * @return 对象
     */
    @Override
    public K entryToObject(DatabaseEntry entry) {
        K result;
        try (Input in = new Input(entry.getData())) {
            result = kryo.readObjectOrNull(in, baseClass);
        }
        return result;
    }

    /**
     * 序列化
     *
     * @param object 对象
     * @param entry  entry
     */
    @Override
    public void objectToEntry(K object, DatabaseEntry entry) {
        try (Output out = new Output(4096, 20971520)) {
            kryo.writeClassAndObject(out, object);
            entry.setData(out.getBuffer());
        }
    }

}
