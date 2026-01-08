package com.lyhn.streamlinedb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// 抽象缓存基类
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock(true);
    }

    protected T get(long key) throws Exception {
        while(true){
            lock.lock();
            if(getting.containsKey(key)){
                // 当前资源被占用
                lock.unlock();
                try {
                    Thread.sleep(1);
                }catch (InterruptedException e){
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            if(cache.containsKey(key)){
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            if(maxResource > 0 && count >= maxResource){
                lock.unlock();
                // 超出最大缓存数量
                throw new Error.CacheFullException();
            }

            count++;
            getting.put(key,true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        }catch (Exception e){
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        references.put(key,1);
        getting.put(key,true);
        cache.put(key,obj);
        lock.unlock();
        return obj;
    }

    // 强行释放一个资源
    protected void release(long key) {
        lock.lock();
        try{
            int ref = references.get(key) - 1;
            if(ref == 0){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }else{
                references.put(key,ref);
            }
        }finally {
            lock.unlock();
        }
    }

    // 关闭缓存
    protected void close(){
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            keys.forEach(key -> {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            });
        }finally {
            lock.unlock();
        }
    }

    protected abstract T getForCache(long key) throws Exception;

    protected abstract void releaseForCache(T pg);
}
