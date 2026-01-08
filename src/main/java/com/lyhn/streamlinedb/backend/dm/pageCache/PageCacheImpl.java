package com.lyhn.streamlinedb.backend.dm.pageCache;

import com.lyhn.streamlinedb.backend.common.AbstractCache;
import com.lyhn.streamlinedb.backend.dm.page.Page;
import com.lyhn.streamlinedb.backend.dm.page.PageImpl;
import com.lyhn.streamlinedb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    // 最少缓存10个页面
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private Lock fileLock;

    private AtomicInteger pageNumbers;

    public PageCacheImpl(int maxResource,RandomAccessFile randomAccessFile,FileChannel fileChannel) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(new RuntimeException("Memory too small"));
        }
        long length = 0;
        try{
            length = randomAccessFile.length();
        }catch (Exception e){
            Panic.panic(e);
        }
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
        this.fileLock = new ReentrantLock();
    }


    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flushPage(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get((int)pgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fileChannel.close();
            randomAccessFile.close();
        }catch (Exception e){
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        releaseInternal((long)page.getPageNumber());
    }

    private void releaseInternal(long pageNumber) {
        super.release(pageNumber);
    }

    // 阶段数据库文件，只保留前maxPageNo个文件
    @Override
    public void truncateByPageNo(int maxPageNo) {
        long size = pageOffset(maxPageNo + 1);
        try {
            randomAccessFile.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    // 将页面数据刷新到磁盘文件
    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fileChannel.position(offset);
            fileChannel.write(buf);
            fileChannel.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno,buf.array(),this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()){
            flushPage(pg);
            pg.setDirty(false);
        }
    }

    private static long pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }
}
