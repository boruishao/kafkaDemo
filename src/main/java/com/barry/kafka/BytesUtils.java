package com.barry.kafka;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/9/9 8:42 PM
 * Version 1.0
 * Describe TODO
 **/

public class BytesUtils {
    public static byte[] long2Bytes(long res) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((res >> offset) & 0xff);
        }
        return buffer;
    }

    public static long bytes2Long(byte[] b) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value <<= 8;
            value |= (b[i] & 0xff);
        }
        return value;
    }
}
