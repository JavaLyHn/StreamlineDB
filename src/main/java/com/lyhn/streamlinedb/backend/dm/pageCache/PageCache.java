package com.lyhn.streamlinedb.backend.dm.pageCache;

import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.dm.page.Page;
import com.lyhn.streamlinedb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    // 页大小为 8KB
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByPageNo(int maxPageNo);
    int getPageNumber();
    void flushPage(Page pg);

    public static PageCacheImpl create(String path,long memory){
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()){
                Panic.panic(new Error.FailCreateFile());
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(new Error.NoFilePermission());
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl((int)memory / PAGE_SIZE,raf,fc);
    }

    public static PageCacheImpl open(String path,long memory){
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        if(!f.exists()){
            Panic.panic(new Error.NoFileExist());
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(new Error.NoFilePermission());
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl((int)memory / PAGE_SIZE,raf,fc);
    }
}
