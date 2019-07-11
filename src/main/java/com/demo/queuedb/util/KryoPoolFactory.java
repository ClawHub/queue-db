package com.demo.queuedb.util;

import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * The type Kryo pool factory.
 * @author lizhiming
 */
public enum KryoPoolFactory {

    /**
     * 初始化字段
     */
    INSTANCE;

    /**
     * The Pool.
     */
    private KryoPool kryoPool;

    /**
     * 类加载时初始化
     */
    KryoPoolFactory() {
        KryoFactory kryoFactory = () -> {
            Kryo kryo = new Kryo();
            //这是对循环引用的支持，可以有效防止栈内存溢出，kryo默认会打开这个属性。
            //确定不会有循环引用发生的时候，可以通过kryo.setReferences(false);关闭循环引用检测，从而提高一些性能
            kryo.setReferences(false);
            //把已知的结构注册到Kryo注册器里面，提高序列化/反序列化效率
            kryo.register(JSONObject.class);
            kryo.register(String.class);
            //显示指定实例化器,首先使用默认无参构造策略DefaultInstantiatorStrategy，若创建对象失败再采用StdInstantiatorStrategy
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            return kryo;
        };
        //通过软连接队列维护Kryo,当垃圾回收时，发现系统内存不足，会优先回收
        kryoPool = new KryoPool.Builder(kryoFactory).softReferences().build();
    }


    /**
     * Gets pool.
     *
     * @return the pool
     */
    public KryoPool getPool() {
        return kryoPool;
    }
}
