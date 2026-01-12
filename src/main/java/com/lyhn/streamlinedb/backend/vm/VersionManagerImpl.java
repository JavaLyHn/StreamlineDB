package com.lyhn.streamlinedb.backend.vm;

import com.lyhn.streamlinedb.backend.common.AbstractCache;
import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.dm.DataManager;
import com.lyhn.streamlinedb.backend.tm.TransactionManager;
import com.lyhn.streamlinedb.backend.tm.TransactionManagerImpl;
import com.lyhn.streamlinedb.backend.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{
    TransactionManager tm;
    DataManager dm;
    // 活跃事务映射表
    Map<Long, Transaction> activeTransaction;
    // 全局锁，保护活跃事务表
    Lock lock;
    // 锁表，管理锁和死锁检测
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    // 读取数据
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        // 获取事务对象
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            // 从缓存中获取数据项
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.nullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            // 判断数据是否对当前事务可见
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    // 插入数据
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        // 获取事务对象
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        // 将数据封装成Entry格式
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    // 删除数据，不是真正的删除数据，而是标记数据为已删除状态
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        // 获取事务对象
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            // 从缓存中获取数据项
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.nullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            // 检查数据项是否对当前事务可见
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                // 获取锁，添加到锁表
                l = lt.add(xid, uid);
            } catch(Exception e) {
                // 死锁检测失败，自动中止事务
                t.err = Error.concurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            // 两阶段锁协议
            if(l != null) {
                l.lock();// 获取锁
                l.unlock();// 释放锁
            }

            // 检查是否已被当前事务删除
            if(entry.getXmax() == xid) {
                return false;
            }

            // 检查版本跳过（是否被其他事务修改），并发冲突检测
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.concurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            // 设置XMAX为当前事务id，标记为已删除
            entry.setXmax(xid);
            // 删除成功
            return true;

        } finally {
            entry.release();
        }
    }

    // 开启新事物
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            // 获取事务id
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    // 提交事务
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        // 从活跃事务表中获取事务对象
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    // 中止事务，回滚所有操作
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }


    // 内部中止方法，支持自动中止
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.nullEntryException;
        }
        return entry;
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
}
