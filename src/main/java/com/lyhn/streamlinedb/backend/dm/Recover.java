package com.lyhn.streamlinedb.backend.dm;

import com.google.common.primitives.Bytes;
import com.lyhn.streamlinedb.backend.common.SubArray;
import com.lyhn.streamlinedb.backend.dm.dataItem.DataItem;
import com.lyhn.streamlinedb.backend.dm.logger.Logger;
import com.lyhn.streamlinedb.backend.dm.page.Page;
import com.lyhn.streamlinedb.backend.dm.page.PageX;
import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;
import com.lyhn.streamlinedb.backend.tm.TransactionManager;
import com.lyhn.streamlinedb.backend.utils.Panic;
import com.lyhn.streamlinedb.backend.utils.Parser;

import java.util.*;

// 在进行增加和修改操作之前，必须先执行对应的日志操作，在保证日志写入磁盘后，才进行数据操作
public class Recover {
    // insert类型
    private static final byte LOG_TYPE_INSERT = 0;
    // update类型
    private static final byte LOG_TYPE_UPDATE = 1;

    // 重做
    private static final int REDO = 0;
    // 撤销
    private static final int UNDO = 1;

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    static class InsertLogInfo {
        // 事务ID
        long xid;
        // 页面号
        int pgno;
        // 页面内偏移量
        short offset;
        // 原始数据
        byte[] raw;// 原始数据
    }

    static class UpdateLogInfo {
        // 事务ID
        long xid;
        // 页面号
        int pgno;
        // 页面内偏移量
        short offset;
        // 原始数据
        byte[] oldRaw;
        // 新数据
        byte[] newRaw;
    }

    // 系统崩溃后恢复数据库到一致状态
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");
        // 重置日志读取位置到开始位置
        lg.rewind();

        // 扫描日志确定最大页面号
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }

        // 如果没有任何日志记录，则保留至少1个页面
        if(maxPgno == 0) {
            maxPgno = 1;
        }

        // 根据扫描到的最大页面号截断数据文件，移除可能存在的未提交事务创建的页面
        pc.truncateByPageNo(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        // 重做已提交事务
        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        // 撤销未完成事务
        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    // 撤销所有未完成事务的操作
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 创建日志缓存{事务id，该事务的所有日志列表}
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        // 扫描所有日志
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            // 处理insert日志
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                // 事务是否活跃（未完成）
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                // 处理update日志
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo（执行时按照顺序执行，撤销则按照倒序）
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            // 遍历所有未完成事务
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 重置日志读取指针到日志文件开头
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            // 处理日志文件
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                // 判断事务是否已提交
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        // 根据操作类型解析日志
        if(flag == REDO) {
            // 使用新数据恢复已提交的事务
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            // 使用旧数据撤销未完成事务的更新
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        // 解析插入日志
        InsertLogInfo li = parseInsertLog(log);
        // 获取对应页面
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                // 撤销操作，将数据标记为无效
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 重做操作，重新执行插入操作
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        // 转换数据项的UID为字节数组
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    // 解析更新日志，提取事务ID、页面号、页面内偏移量、旧数据和新数据
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        // 事务id
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        // 页面号
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        // 页面内偏移量
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        // 原始数据
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }
}
