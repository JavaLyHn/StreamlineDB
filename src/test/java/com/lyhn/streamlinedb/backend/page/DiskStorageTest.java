package com.lyhn.streamlinedb.backend.page;

import com.lyhn.streamlinedb.backend.dm.page.Page;
import com.lyhn.streamlinedb.backend.dm.page.PageOne;
import com.lyhn.streamlinedb.backend.dm.page.PageX;
import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;
import org.junit.Test;

public class DiskStorageTest {
    private static final String DB_PATH = "E:\\StreamlineDB\\test_db";
    private static final int MEMORY = 10 * 1024 * 1024;

    @Test
    public void test1_createDatabase() throws Exception {
        System.out.println("测试1: 创建数据库文件");

        PageCache pc = PageCache.create(DB_PATH, MEMORY);

        assert pc.getPageNumber() == 0 : "新数据库应该有0个页面";

        pc.close();

        System.out.println("✓ 数据库文件创建成功");
        System.out.println("✓ PageCache 初始化成功\n");
    }

    @Test
    public void test2_pageOneVerification() throws Exception {
        System.out.println("测试2: PageOne 验证机制");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        byte[] initData = PageOne.InitRaw();
        int pgno = pc.newPage(initData);

        Page page = pc.getPage(pgno);

        assert !PageOne.checkVc(page) : "刚创建的PageOne验证码不应该匹配（open状态）";

        PageOne.setVcClose(page);
        assert PageOne.checkVc(page) : "设置close后验证码应该匹配";

        pc.release(page);

        pc.close();

        pc = PageCache.open(DB_PATH, MEMORY);
        page = pc.getPage(pgno);
        assert PageOne.checkVc(page) : "重新打开后验证码应该仍然匹配";
        pc.release(page);

        pc.close();

        System.out.println("✓ PageOne 验证机制正常");
        System.out.println("✓ 验证码设置和检查功能正常\n");
    }

    @Test
    public void test3_createAndWritePage() throws Exception {
        System.out.println("测试3: 创建新页面并写入数据");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        byte[] initData = PageX.InitRaw();
        int pgno = pc.newPage(initData);

        Page page = pc.getPage(pgno);

        byte[] data = "Hello, World!".getBytes();
        short offset = PageX.insert(page, data);

        assert offset == 2 : "数据应该在偏移量2处开始";

        byte[] readData = new byte[data.length];
        System.arraycopy(page.getData(), offset, readData, 0, data.length);

        assert new String(readData).equals("Hello, World!") : "读取的数据应该与写入的数据一致";

        pc.release(page);
        pc.close();

        System.out.println("✓ 新页面创建成功");
        System.out.println("✓ 数据写入成功");
        System.out.println("✓ 数据读取验证成功\n");
    }

    @Test
    public void test4_readFromDisk() throws Exception {
        System.out.println("测试4: 从磁盘读取页面");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        Page page = pc.getPage(4);

        byte[] data = "Hello, World!".getBytes();
        byte[] readData = new byte[data.length];
        System.arraycopy(page.getData(), 2, readData, 0, data.length);

        assert new String(readData).equals("Hello, World!") : "从磁盘读取的数据应该正确";

        pc.release(page);
        pc.close();

        System.out.println("✓ 从磁盘读取页面成功");
        System.out.println("✓ 数据持久化验证成功\n");
    }

    @Test
    public void test5_dirtyPageTracking() throws Exception {
        System.out.println("测试5: 脏页跟踪机制");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        byte[] initData = PageX.InitRaw();
        int pgno = pc.newPage(initData);

        Page page = pc.getPage(pgno);

        assert !page.isDirty() : "新页面不应该是脏页";

        page.lock();
        page.getData()[0] = 'X';
        page.setDirty(true);
        page.unlock();

        assert page.isDirty() : "修改后的页面应该是脏页";

        pc.release(page);
        pc.close();

        pc = PageCache.open(DB_PATH, MEMORY);
        page = pc.getPage(pgno);

        assert page.getData()[0] == 'X' : "脏页应该已经刷盘";
        assert !page.isDirty() : "重新加载的页面不应该是脏页";

        pc.release(page);
        pc.close();

        System.out.println("✓ 脏页标记功能正常");
        System.out.println("✓ 脏页刷盘功能正常");
        System.out.println("✓ 页面释放后脏页自动刷盘\n");
    }

    @Test
    public void test6_pageCaching() throws Exception {
        System.out.println("测试6: 页面缓存机制");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        Page page1 = pc.getPage(4);
        Page page2 = pc.getPage(4);

        assert page1 == page2 : "相同页面的多次获取应该返回同一个对象";

        pc.release(page1);
        pc.release(page2);

        page1 = pc.getPage(4);
        assert page1 != page2 : "释放后重新获取应该创建新对象";

        pc.release(page1);
        pc.close();

        System.out.println("✓ 页面缓存功能正常");
        System.out.println("✓ 缓存命中机制正常");
        System.out.println("✓ 缓存释放机制正常\n");
    }

    @Test
    public void test7_concurrentAccess() throws Exception {
        System.out.println("测试7: 并发访问测试");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        byte[] initData = PageX.InitRaw();
        final int pgno = pc.newPage(initData);

        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    Page page = pc.getPage(pgno);
                    page.lock();
                    page.getData()[threadId] = (byte)('A' + threadId);
                    page.setDirty(true);
                    page.unlock();
                    pc.release(page);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Page page = pc.getPage(pgno);
        for (int i = 0; i < threads.length; i++) {
            assert page.getData()[i] == (byte)('A' + i) : "线程" + i + "的数据应该正确";
        }
        pc.release(page);

        pc.close();

        System.out.println("✓ 并发访问测试完成");
        System.out.println("✓ 线程安全验证成功\n");
    }

    @Test
    public void test8_persistence() throws Exception {
        System.out.println("测试8: 数据持久化验证");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        byte[] pageOneData = PageOne.InitRaw();
        int pageOnePgno = pc.newPage(pageOneData);

        byte[] initData = PageX.InitRaw();
        String persistentData = "Persistent Data Test";
        System.arraycopy(persistentData.getBytes(), 0, initData, 0, persistentData.getBytes().length);

        int pgno = pc.newPage(initData);

        Page pageOne = pc.getPage(pageOnePgno);
        assert !PageOne.checkVc(pageOne) : "刚创建的PageOne验证码不应该匹配（open状态）";
        PageOne.setVcClose(pageOne);
        pc.release(pageOne);

        pc.close();

        pc = PageCache.open(DB_PATH, MEMORY);
        Page page = pc.getPage(pgno);
        String readData = new String(page.getData(), 0, persistentData.getBytes().length);

        assert readData.equals(persistentData) : "持久化数据应该正确";

        pageOne = pc.getPage(pageOnePgno);
        assert PageOne.checkVc(pageOne) : "PageOne 验证码应该持久化";
        pc.release(pageOne);

        pc.release(page);
        pc.close();

        System.out.println("✓ 数据持久化验证成功");
        System.out.println("✓ 数据库关闭后数据仍然存在");
        System.out.println("✓ 重新打开数据库数据正确\n");
    }

    @Test
    public void test9_pageXIntegration() throws Exception {
        System.out.println("测试9: PageX 集成测试");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        byte[] initData = PageX.InitRaw();
        int pgno = pc.newPage(initData);

        Page page = pc.getPage(pgno);

        byte[] data1 = "First Data".getBytes();
        short offset1 = PageX.insert(page, data1);
        System.out.println("  插入数据1: " + new String(data1) + ", 位置: " + offset1);

        byte[] data2 = "Second Data".getBytes();
        short offset2 = PageX.insert(page, data2);
        System.out.println("  插入数据2: " + new String(data2) + ", 位置: " + offset2);

        int freeSpace = PageX.getFreeSpace(page);
        System.out.println("  剩余空闲空间: " + freeSpace + " 字节");

        byte[] readData1 = new byte[data1.length];
        System.arraycopy(page.getData(), offset1, readData1, 0, data1.length);
        assert new String(readData1).equals("First Data") : "数据1应该正确";

        byte[] readData2 = new byte[data2.length];
        System.arraycopy(page.getData(), offset2, readData2, 0, data2.length);
        assert new String(readData2).equals("Second Data") : "数据2应该正确";

        pc.release(page);
        pc.close();

        pc = PageCache.open(DB_PATH, MEMORY);
        page = pc.getPage(pgno);

        readData1 = new byte[data1.length];
        System.arraycopy(page.getData(), offset1, readData1, 0, data1.length);
        assert new String(readData1).equals("First Data") : "持久化后数据1应该正确";

        readData2 = new byte[data2.length];
        System.arraycopy(page.getData(), offset2, readData2, 0, data2.length);
        assert new String(readData2).equals("Second Data") : "持久化后数据2应该正确";

        pc.release(page);
        pc.close();

        System.out.println("✓ PageX 数据插入功能正常");
        System.out.println("✓ PageX 空间管理功能正常");
        System.out.println("✓ PageX 数据持久化功能正常\n");
    }
}
