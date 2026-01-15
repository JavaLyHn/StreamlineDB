package com.lyhn.streamlinedb.backend.tbm;

import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.dm.DataManager;
import com.lyhn.streamlinedb.backend.parser.statement.*;
import com.lyhn.streamlinedb.backend.utils.Parser;
import com.lyhn.streamlinedb.backend.vm.VersionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// 负责数据库中的表结构
public class TableManagerImpl implements TableManager{
    VersionManager vm;
    DataManager dm;
    // 引导器，存储元数据信息
    private Booter booter;
    // 表名到表对象的缓存
    private Map<String, Table> tableCache;
    // 事务id到表列表的缓存，记录每个事务创建了哪些表
    private Map<Long, List<Table>> xidTableCache;
    // 全局锁，保护表缓存
    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    // 加载所有已存在的表
    private void loadTables() {
        // 获取第一个表的uid
        long uid = firstTableUid();
        while(uid != 0) {
            // 循环加载所有表并加入缓存
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    // 开启新事务
    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        // 根据隔离级别并设置事务级别
        int level = begin.isRepeatableRead?1:0;
        // 调用vm开启事务
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        // 返回事务id和操作结果
        return res;
    }

    // 提交事务
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    // 中止事务
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    // 显示所有表信息
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            // 遍历全局缓存中的所有表
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            // 获取当前事务创建的表
            List<Table> t = xidTableCache.get(xid);
            // 如果当前事务没有创建任何表，直接返回空字符串
            if(t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    // 创建新表
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            // 检查表名是否存在
            if(tableCache.containsKey(create.tableName)) {
                throw Error.duplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            // 更新引导器中的第一个表uid
            updateFirstTableUid(table.uid);
            // 将表加入缓存
            tableCache.put(create.tableName, table);
            // 检查事务缓存中是否存在该事务的记录
            if(!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            // 将表加入事务缓存
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    // 插入数据
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        // 根据表名查找表对象
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.tableNotFoundException;
        }
        // 调用表的insert操作
        table.insert(xid, insert);
        // 返回操作结果
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        // 根据表名查找表对象
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.tableNotFoundException;
        }
        // 调用表的read操作并返回操作结果
        return table.read(xid, read).getBytes();
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.tableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.tableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }
}
