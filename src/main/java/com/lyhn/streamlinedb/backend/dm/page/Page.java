package com.lyhn.streamlinedb.backend.dm.page;

// 定义Page页的操作
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
