package com.lyhn.streamlinedb.backend.dm.pageIndex;

import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// 页面空闲空间索引管理器
public class PageIndex {
    // 将一个页划分为40个区间
    private static final int INTERVALS_NO = 40;
    // 每个区间的大小阈值
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex(){
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i <= INTERVALS_NO; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno,int freeSpace){
        lock.lock();
        try {
            // 页面空闲空间所在的页面编号
            int num = freeSpace / THRESHOLD;
            lists[num].add(new PageInfo(pgno,freeSpace));
        }finally {
            lock.unlock();
        }
    }

    public PageInfo select(int spaceSize){
        lock.lock();
        try {
            int num = spaceSize / THRESHOLD;
            if(num < INTERVALS_NO){
                num++;
            }
            while(num <= INTERVALS_NO){
                if(lists[num].size() == 0){
                    num++;
                    continue;
                }
                return lists[num].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }
    }
}
