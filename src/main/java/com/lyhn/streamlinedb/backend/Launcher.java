package com.lyhn.streamlinedb.backend;

import com.lyhn.streamlinedb.backend.dm.DataManager;
import com.lyhn.streamlinedb.backend.server.Server;
import com.lyhn.streamlinedb.backend.tbm.TableManager;
import com.lyhn.streamlinedb.backend.tm.TransactionManager;
import com.lyhn.streamlinedb.backend.utils.Panic;
import com.lyhn.streamlinedb.backend.vm.VersionManager;
import com.lyhn.streamlinedb.backend.vm.VersionManagerImpl;
import org.apache.commons.cli.*;
import com.lyhn.streamlinedb.backend.common.Error;

public class Launcher {
    /**
     * Transaction Manager:TM 通过维护 XID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态。
     * Data Manager:DM 直接管理数据库 DB 文件和日志文件。DM 的主要职责有: 1) 分页管理 DB 文件，并进行缓存；
     *                                                             2) 管理日志文件，保证在发生错误时可以根据日志进行恢复；
     *                                                             3) 抽象 DB 文件为 DataItem 供上层模块使用，并提供缓存。
     * Version Manager:VM 基于两段锁协议实现了调度序列的可串行化，并实现了 MVCC 以消除读写阻塞。同时实现了两种隔离级别。
     * Index Manager:IM 实现了基于 B+ 树的索引，BTW，目前 where 只支持已索引字段。
     * Table Manager:TBM 实现了对字段和表的管理。同时，解析 SQL 语句，并根据语句操作表。
     */

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        // 用来注册所有程序支持的命令行参数
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        options.addOption("optimized", false, "-optimized Enable Clock-Sweep buffer pool");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        boolean optimized = cmd.hasOption("optimized");

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")), optimized);
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"), optimized);
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    private static void createDB(String path, boolean optimized) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm;
//        if (optimized) {
            // dm = DataManager.createOptimized(path, DEFALUT_MEM, tm);
//        } else {
            dm = DataManager.create(path, DEFALUT_MEM, tm);
//        }
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem, boolean optimized) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm;
//        if (optimized) {
            // dm = DataManager.openOptimized(path, mem, tm);
//        } else {
            dm = DataManager.open(path, mem, tm);
//        }
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.invalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.invalidMemException);
        }
        return DEFALUT_MEM;
    }
}
