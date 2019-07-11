package com.demo.queuedb.util;

import java.nio.ByteBuffer;
import java.util.Objects;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The type Byte buffer util.
 *
 * @author lizhiming
 */
public class ByteBufferUtil {
    /**
     * ByteBuffer转byte[]
     *
     * @param byteBuffer byteBuffer
     * @return the byte[]
     */
    public static byte[] byteBufferToBytes(final ByteBuffer byteBuffer) {
        final int len = byteBuffer.limit() - byteBuffer.position();
        final byte[] bytes = new byte[len];
        if (byteBuffer.isReadOnly()) {
            return null;
        }
        byteBuffer.get(bytes);
        return bytes;
    }

    /**
     * ByteBuffer转String
     *
     * @param byteBuffer byteBuffer
     * @return string
     */
    public static String byteBufferToString(ByteBuffer byteBuffer) {
        return new String(Objects.requireNonNull(byteBufferToBytes(byteBuffer)));
    }


    /**
     * String转ByteBuffer
     *
     * @param value String
     * @return ByteBuffer
     */
    public static ByteBuffer stringToByteBuffer(String value) {
        final byte[] longArray = value.getBytes(UTF_8);
        return bytesToByteBuffer(longArray);
    }

    /**
     * Long转ByteBuffer
     *
     * @param value Long
     * @return ByteBuffer
     */
    public static ByteBuffer longToByteBuffer(Long value) {
        return bytesToByteBuffer(longToBytes(value));
    }

    /**
     * byte[] 转 ByteBuffer
     * 堆外内存
     *
     * @param bytes byte[]
     * @return ByteBuffer
     */
    public static ByteBuffer bytesToByteBuffer(byte[] bytes) {
        ByteBuffer buffer = allocateDirect(bytes.length);
        //一定要执行flip
        buffer.put(bytes).flip();
        return buffer;
    }

    /**
     * Long转 byte[].
     * 堆内存
     *
     * @param value Long
     * @return byte[]
     */
    public static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    /**
     * byte[]转 Long.
     * 堆内存
     *
     * @param bytes the bytes
     * @return the long
     */
    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8).put(bytes, 0, bytes.length);
        //一定要执行flip
        buffer.flip();
        return buffer.getLong();
    }

}
