package com.lyhn.streamlinedb.backend.im;

import com.lyhn.streamlinedb.backend.common.SubArray;
import com.lyhn.streamlinedb.backend.dm.dataItem.DataItem;
import com.lyhn.streamlinedb.backend.tm.TransactionManagerImpl;
import com.lyhn.streamlinedb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node负责处理节点的插入、分裂和查找等操作
 * Node结构如下：
 * [LeafFlag（标识节点是否为叶子节点）][KeyNumber（节点中键的数量）][SiblingUid（兄弟节点的UID）]
 * [Son0][Key0]（子节点引用和键值的交替排列）[Son1][Key1]...[SonN][KeyN]
 */


public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    // 节点头大小
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    // 平衡因子
    static final int BALANCE_NUMBER = 32;
    // 节点总大小
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    // 节点的唯一标识符
    long uid;

    // 设置和获取叶子节点标志
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    // 设置和获取键数量
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    // 设置和获取兄弟节点UID
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    // 设置和获取第k个子节点
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }
    // 设置和获取第k个键
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    // 从指定位置复制数据
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    // 从指定位置开始移动数据（为插入腾出空间）
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    // 创建新的根节点（分裂时使用）
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        // 根节点无兄弟节点
        setRawSibling(raw, 0);
        // 设置第一个子节点
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        // 设置第二各子节点
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    public static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    // 从存储加载节点
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    // 查询给定键的下一个节点
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            // 遍历节点中的所有键
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);
                if(key < ik) {
                    // 找到第一个大于给定键的位置
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    // 返回对应的子节点引用
                    return res;
                }
            }
            res.uid = 0;
            // 未找到则返回兄弟节点引用
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    // 范围查询（叶子节点专用）
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while(kth < noKeys) {
                // 找到第一个大于等于左边界的位置
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            // 收集所有符合条件的子节点引用
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            // 如果遍历到末尾，返回兄弟节点引用
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    // 插入和分裂操作
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            // 尝试插入新的键值对
            success = insert(uid, key);
            if(!success) {
                // 如果插入失败（需要插入到兄弟节点），返回兄弟节点引用
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            // 如果插入成功但节点已满，执行分裂操作
            if(needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                // 返回分裂后的新节点引用和分裂键
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        if(getRawIfLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    // 分裂操作
    private SplitRes split() throws Exception {
        // 创建新节点
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
