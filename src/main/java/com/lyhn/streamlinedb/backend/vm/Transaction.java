package com.lyhn.streamlinedb.backend.vm;

import com.lyhn.streamlinedb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

// vm对一个事务的抽象
public class Transaction {
    // 事务id
    public long xid;
    // 事务级别
    public int level;
    // 用于存储在事务开始时活跃（未提交）的事务id
    // 快照的作用是在可重复读的隔离级别下，确保事务在执行过程中读取到的数据是一致的
    public Map<Long, Boolean> snapshot;
    // 事务执行过程中的错误信息
    public Exception err;
    // 标记事务是否会自动中止
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        // 创建一个新的事务对象
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 如果隔离级别为0（读已提交），不需要快照
        // 如果隔离级别为1（可重复读），需要快照，记录当前所有活跃事务的id
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    // 检查指定事务id是否在当前事务的快照中
    public boolean isInSnapshot(long xid) {
        // 排除超级事务（特殊事务，用于系统内部操作）id
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }

}
