package com.lyhn.streamlinedb.backend.vm;

import com.lyhn.streamlinedb.backend.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 * 节点：xid和uid
 * 边：事务持有资源（xid->uid）和事务等待资源（xid->uid）
 */
public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 添加锁请求，实现了锁的获取和死锁检测
    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 如果事务已经持有该资源，直接返回
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            // 如果资源未被占用，分配给事务
            if (!u2x.containsKey(uid)) {
                // 记录资源被哪一个事务占用
                u2x.put(uid, xid);
                // 记录事务持有哪些资源
                putIntoList(x2u, xid, uid);
                // 获取锁成功，返回null
                return null;
            }
            // 资源已被占用，将事务加入到等待队列
            waitU.put(xid, uid);
            // 记录资源有哪些事务在等待
            putIntoList(wait, uid, xid);
            // 检查是否产生死锁
            if (hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.deadlockException;
            }
            Lock l = new ReentrantLock();
            // 锁住，进入阻塞状态
            l.lock();
            // 记录事务的锁对象
            waitLock.put(xid, l);
            // 不产生死锁，创建一个锁对象并返回，事务将被阻塞
            return l;

        } finally {
            lock.unlock();
        }
    }

    // 处理事务释放锁
    public void remove(long xid) {
        lock.lock();
        try {
            // 获取事务持有的所有资源
            List<Long> l = x2u.get(xid);
            if(l != null) {
                // 对每一个资源，从等待队列中选择一个新的事务来占用
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            // 清除事务的所有相关数据结构
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        // 释放资源
        u2x.remove(uid);
        // 获取等待该资源的事务列表
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            long xid = l.remove(0);
            // 检查该事务是否在等待
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                // 获取并移除事务的锁对象
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                try {
                    lo.unlock();
                } catch (IllegalMonitorStateException e) {
                }
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }

    // 记录每一个事务的时间戳
    // xid -> stamp
    private Map<Long, Integer> xidStamp;
    // 全局时间戳计数器
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        // 为每一个事务分配一个时间戳 初始化为1
        stamp = 1;
        // 遍历所有事务
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            // 对每一个事务执行DFS检测
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        // 发现环（死锁）
        if(stp != null && stp == stamp) {
            return true;
        }
        // 已在前几轮DFS中访问过
        if(stp != null && stp < stamp) {
            return false;// 避免重复检测
        }

        // 首次访问
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }
}
