package com.lyhn.streamlinedb.backend.logger;


import com.lyhn.streamlinedb.backend.dm.logger.Logger;
import org.junit.Test;

import java.io.File;

public class LoggerTest {
    private static final String LOG_PATH = "E:\\StreamlineDB\\test_logger";
    private static final int MEMORY = 10 * 1024 * 1024;

    @Test
    public void cleanup() {
        File logFile = new File(LOG_PATH + ".log");
        if (logFile.exists()) {
            logFile.delete();
        }
        File logFileChecksum = new File(LOG_PATH + "_checksum.log");
        if (logFileChecksum.exists()) {
            logFileChecksum.delete();
        }
        File logFileReopen = new File(LOG_PATH + "_reopen.log");
        if (logFileReopen.exists()) {
            logFileReopen.delete();
        }
        File logFileConcurrent = new File(LOG_PATH + "_concurrent.log");
        if (logFileConcurrent.exists()) {
            logFileConcurrent.delete();
        }
        File logFileViewAll = new File(LOG_PATH + "_viewall.log");
        if (logFileViewAll.exists()) {
            logFileViewAll.delete();
        }
    }

    @Test
    public void test1_createLogger() throws Exception {
        System.out.println("测试1: 创建日志文件");

        Logger logger = Logger.create(LOG_PATH);

        logger.close();

        System.out.println("✓ 日志文件创建成功\n");
    }

    @Test
    public void test2_writeLog() throws Exception {
        System.out.println("测试2: 写入日志");

        Logger logger = Logger.open(LOG_PATH);

        byte[] data1 = "First log entry".getBytes();
        logger.log(data1);
        System.out.println("  写入日志1: " + new String(data1));

        byte[] data2 = "Second log entry".getBytes();
        logger.log(data2);
        System.out.println("  写入日志2: " + new String(data2));

        byte[] data3 = "Third log entry".getBytes();
        logger.log(data3);
        System.out.println("  写入日志3: " + new String(data3));

        logger.close();

        System.out.println("✓ 日志写入成功\n");
    }

    @Test
    public void test3_readLog() throws Exception {
        System.out.println("测试3: 读取日志");

        Logger logger = Logger.open(LOG_PATH);

        logger.rewind();

        byte[] log1 = logger.next();
        assert log1 != null : "应该能读取到第一条日志";
        assert new String(log1).equals("First log entry") : "第一条日志内容应该正确";
        System.out.println("  读取日志1: " + new String(log1));

        byte[] log2 = logger.next();
        assert log2 != null : "应该能读取到第二条日志";
        assert new String(log2).equals("Second log entry") : "第二条日志内容应该正确";
        System.out.println("  读取日志2: " + new String(log2));

        byte[] log3 = logger.next();
        assert log3 != null : "应该能读取到第三条日志";
        assert new String(log3).equals("Third log entry") : "第三条日志内容应该正确";
        System.out.println("  读取日志3: " + new String(log3));

        byte[] log4 = logger.next();
        assert log4 == null : "不应该有第四条日志";

        logger.close();

        System.out.println("✓ 日志读取成功\n");
    }

    @Test
    public void test4_truncateLog() throws Exception {
        System.out.println("测试4: 截断日志");

        Logger logger = Logger.open(LOG_PATH);

        logger.rewind();
        byte[] log1 = logger.next();
        logger.next();

        long position = 4 + 8 + log1.length;

        logger.truncate(position);

        logger.rewind();
        log1 = logger.next();
        assert log1 != null : "应该能读取到第一条日志";
        assert new String(log1).equals("First log entry") : "第一条日志内容应该正确";
        System.out.println("  截断后剩余日志1: " + new String(log1));

        byte[] log2 = logger.next();
        assert log2 == null : "截断后不应该有第二条日志";

        logger.close();

        System.out.println("✓ 日志截断成功\n");
    }

    @Test
    public void test5_reopenLogger() throws Exception {
        System.out.println("测试5: 重新打开日志文件");

        Logger logger = Logger.create(LOG_PATH + "_reopen");

        byte[] data = "Reopen test log".getBytes();
        logger.log(data);
        logger.close();

        logger = Logger.open(LOG_PATH + "_reopen");
        logger.rewind();

        byte[] log = logger.next();
        assert log != null : "应该能读取到日志";
        assert new String(log).equals("Reopen test log") : "日志内容应该正确";
        System.out.println("  重新打开后读取日志: " + new String(log));

        logger.close();

        System.out.println("✓ 日志文件重新打开成功\n");
    }

    @Test
    public void test6_checksumVerification() throws Exception {
        System.out.println("测试6: 校验和验证");

        Logger logger = Logger.create(LOG_PATH + "_checksum");

        byte[] data1 = "Checksum test data 1".getBytes();
        logger.log(data1);

        byte[] data2 = "Checksum test data 2".getBytes();
        logger.log(data2);

        logger.close();

        logger = Logger.open(LOG_PATH + "_checksum");
        logger.rewind();

        byte[] log1 = logger.next();
        assert log1 != null : "应该能读取到第一条日志";
        assert new String(log1).equals("Checksum test data 1") : "第一条日志内容应该正确";
        System.out.println("  校验和验证通过，日志1: " + new String(log1));

        byte[] log2 = logger.next();
        assert log2 != null : "应该能读取到第二条日志";
        assert new String(log2).equals("Checksum test data 2") : "第二条日志内容应该正确";
        System.out.println("  校验和验证通过，日志2: " + new String(log2));

        logger.close();

        System.out.println("✓ 校验和验证成功\n");
    }

    @Test
    public void test7_concurrentWrite() throws Exception {
        System.out.println("测试7: 并发写入");

        Logger logger = Logger.create(LOG_PATH + "_concurrent");
//        Logger logger = Logger.open(LOG_PATH + "_concurrent");
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    byte[] data = ("Concurrent log from thread " + threadId).getBytes();
                    logger.log(data);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        logger.rewind();

        int logCount = 0;
        byte[] log;
        while ((log = logger.next()) != null) {
            logCount++;
            System.out.println("  读取日志" + logCount + ": " + new String(log));
        }
        System.out.println(logCount);
        assert logCount == 5 : "应该有5条日志";

        logger.close();

        System.out.println("✓ 并发写入测试成功\n");
    }

    @Test
    public void test8_viewAllLogs() throws Exception {
        System.out.println("测试8: 查看所有日志");

        Logger logger = Logger.open(LOG_PATH + "_concurrent");
        logger.rewind();

        System.out.println("  开始遍历所有日志:");
        int logCount = 0;
        byte[] log;
        while ((log = logger.next()) != null) {
            logCount++;
            System.out.println("    日志" + logCount + ": " + new String(log));
        }

        System.out.println("  总共读取到 " + logCount + " 条日志");

        logger.close();

        System.out.println("✓ 查看所有日志测试成功\n");
    }
}
