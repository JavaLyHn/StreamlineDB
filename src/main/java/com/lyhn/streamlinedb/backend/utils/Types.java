package com.lyhn.streamlinedb.backend.utils;

public class Types {
    // 将页面号和偏移量组合成一个64位的唯一标识符
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
