package com.lyhn.streamlinedb.backend.page;

import com.lyhn.streamlinedb.backend.dm.page.Page;
import com.lyhn.streamlinedb.backend.dm.page.PageX;
import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;
import org.junit.Test;

public class PageXTest {
    private static final String DB_PATH = "E:\\StreamlineDB\\test_db1";
    private static final int MEMORY = 10 * 1024 * 1024;

    @Test
    public void test1_insert() throws Exception {
        System.out.println("测试1: 插入数据到页面");

        PageCache pc = PageCache.create(DB_PATH, MEMORY);

        byte[] initData = PageX.InitRaw();
        int pgno = pc.newPage(initData);

        Page page = pc.getPage(pgno);

        byte[] data1 = "Hello, World!".getBytes();
        short offset1 = PageX.insert(page, data1);
        System.out.println("  插入数据1: " + new String(data1) + ", 位置: " + offset1);

        byte[] data2 = "This is PageX test.".getBytes();
        short offset2 = PageX.insert(page, data2);
        System.out.println("  插入数据2: " + new String(data2) + ", 位置: " + offset2);

        assert offset1 == 2 : "第一个数据应该在偏移量2处";
        assert offset2 == offset1 + data1.length : "第二个数据应该紧跟在第一个数据后面";

        pc.release(page);
        pc.close();

        System.out.println("✓ 数据插入测试成功\n");
    }

    @Test
    public void test2_getFreeSpace() throws Exception {
        System.out.println("测试2: 获取空闲空间");

        PageCache pc = PageCache.open(DB_PATH, MEMORY);

        byte[] initData = PageX.InitRaw();
        int pgno = pc.newPage(initData);

        Page page = pc.getPage(pgno);

        int freeSpace = PageX.getFreeSpace(page);
        System.out.println("  当前空闲空间: " + freeSpace + " 字节");

        byte[] data = new byte[100];
        PageX.insert(page, data);

        int freeSpaceAfter = PageX.getFreeSpace(page);
        System.out.println("  插入100字节后空闲空间: " + freeSpaceAfter + " 字节");

        assert freeSpaceAfter == freeSpace - 100 : "空闲空间应该减少100字节";

        pc.release(page);
        pc.close();

        System.out.println("✓ 空闲空间测试成功\n");
    }

    @Test
    public void test3_recoverInsert() throws Exception {
        System.out.println("测试3: 恢复插入");

        PageCache pc = PageCache.create(DB_PATH + "_recover", MEMORY);

        byte[] initData = PageX.InitRaw();
        int pgno = pc.newPage(initData);

        Page page = pc.getPage(pgno);

        byte[] data1 = "Data 1".getBytes();
        PageX.insert(page, data1);

        byte[] data2 = "Data 2".getBytes();
        short offset = (short)(2 + data1.length);
        PageX.recoverInsert(page, data2, offset);

        byte[] readData = new byte[data2.length];
        System.arraycopy(page.getData(), offset, readData, 0, data2.length);

        assert new String(readData).equals("Data 2") : "恢复插入的数据应该正确";

        pc.release(page);
        pc.close();

        System.out.println("✓ 恢复插入测试成功\n");
    }

    @Test
    public void test4_recoverUpdate() throws Exception {
        System.out.println("测试4: 恢复更新");

        PageCache pc = PageCache.create(DB_PATH + "_update", MEMORY);

        byte[] initData = PageX.InitRaw();
        int pgno = pc.newPage(initData);

        Page page = pc.getPage(pgno);

        byte[] data1 = "Original Data".getBytes();
        short offset = PageX.insert(page, data1);

        byte[] data2 = "Updated Data".getBytes();
        PageX.recoverUpdate(page, data2, offset);

        byte[] readData = new byte[data2.length];
        System.arraycopy(page.getData(), offset, readData, 0, data2.length);

        assert new String(readData).equals("Updated Data") : "恢复更新的数据应该正确";

        pc.release(page);
        pc.close();

        System.out.println("✓ 恢复更新测试成功\n");
    }
}
