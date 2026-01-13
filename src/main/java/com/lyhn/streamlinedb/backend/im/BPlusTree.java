package com.lyhn.streamlinedb.backend.im;


import com.lyhn.streamlinedb.backend.common.SubArray;
import com.lyhn.streamlinedb.backend.dm.DataManager;
import com.lyhn.streamlinedb.backend.dm.dataItem.DataItem;
import com.lyhn.streamlinedb.backend.tm.TransactionManagerImpl;
import com.lyhn.streamlinedb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// 负责管理整个B+树的创建、加载、查找和插入操作
public class BPlusTree {
    DataManager dm;
    // 引导节点的uid，存储根节点的uid
    long bootUid;
    // 引导节点的数据项
    DataItem bootDataItem;
    // 引导节点的锁，用于并发控制
    Lock bootLock;

    // 创建和加载B+树
    public static long create(DataManager dm) throws Exception {
        // 创建空的根节点
        byte[] rawRoot = Node.newNilRootRaw();
        // 将根节点的uid存储到引导节点中
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        // 返回引导节点的uid
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    // 加载B+树，根据引导节点uid加载已存在的B+树
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        // 根据引导节点uid读取数据
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        // 初始化B+树对象
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    // 获取根节点uid
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            // 从引导节点数据中解析出根节点uid
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    // 更新根节点（当根节点分裂时使用）
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            // 创建新的根节点
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            // 更新引导节点数据中的根节点uid
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    // 递归查找包含指定键的叶子节点
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            // 是内部节点则递归继续查找
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }
    // 找到下一个要访问的子节点
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            // 继续处理兄弟节点
            nodeUid = res.siblingUid;
        }
    }

    // 实际上是调用范围查询，左右边界相同
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    // 调用范围查询
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        // 如果根节点分裂，更新根节点
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            // 是叶节点就直接插入
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            // 是内部节点，递归插入到子节点
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                // 处理子节点分裂的情况
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
