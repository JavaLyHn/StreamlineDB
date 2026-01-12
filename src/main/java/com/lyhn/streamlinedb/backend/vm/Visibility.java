package com.lyhn.streamlinedb.backend.vm;

import com.lyhn.streamlinedb.backend.tm.TransactionManager;

public class Visibility {
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            // 读已提交，不用检查
            return false;
        } else {
            // 可重复读
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();// 创建事务id
        long xmax = e.getXmax();// 删除/更新事务id
        if(xmin == xid && xmax == 0) return true;

        // 创建事务是否已提交
        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        // 当前事务id
        long xid = t.xid;
        // 创建事务id
        long xmin = e.getXmin();
        // 删除/更新该数据的事务id，0表示未删除
        long xmax = e.getXmax();
        // 如果事务时当前事务创建的，且未被删除，则可见
        // 事务T1插入数据后，T1自己可以读取到这条数据
        if(xmin == xid && xmax == 0) return true;

        // 创建事务已提交 && 创建事务在当前事务之前开始 && 创建事务不在当前事务的快照中（创建事务在当前事务开始之前已经提交）
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 未删除 可见
            if(xmax == 0) return true;
            // 不是当前事务删除的
            if(xmax != xid) {
                // 删除事务未提交 || 删除事务在当前事务之后开始 || 删除事务在当前事务的快照中（删除事务在当前事务开始之后才提交）
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
