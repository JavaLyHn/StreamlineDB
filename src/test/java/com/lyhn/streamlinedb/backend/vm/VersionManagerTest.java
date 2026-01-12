package com.lyhn.streamlinedb.backend.vm;

import com.lyhn.streamlinedb.backend.dm.DataManager;
import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;
import com.lyhn.streamlinedb.backend.tm.TransactionManager;
import com.lyhn.streamlinedb.backend.utils.Panic;
import com.lyhn.streamlinedb.backend.utils.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class VersionManagerTest {
    private static final String TEST_PATH = "E:\\StreamlineDB\\VersionManagerTest";
    private TransactionManager tm;
    private DataManager dm;
    private VersionManager vm;

    public VersionManagerTest() {
        Panic.setTestMode(true);
    }

    @Before
    public void setUp() throws Exception {
        cleanupTestFiles();
        tm = TransactionManager.create(TEST_PATH);
        dm = DataManager.create(TEST_PATH, PageCache.PAGE_SIZE * 10, tm);
        vm = VersionManager.newVersionManager(tm, dm);
    }

    @After
    public void tearDown() throws Exception {
        if (dm != null) dm.close();
        if (tm != null) tm.close();
        cleanupTestFiles();
    }

    private void cleanupTestFiles() {
        new File(TEST_PATH + ".db").delete();
        new File(TEST_PATH + ".log").delete();
        new File(TEST_PATH + ".xid").delete();
    }

    @Test
    public void testBasicInsert() throws Exception {
        long xid = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid, data);

        Assert.assertTrue("UID should be positive", uid > 0);

        byte[] readData = vm.read(xid, uid);
        Assert.assertNotNull("Read data should not be null", readData);
        Assert.assertTrue("Read data should match inserted data", Arrays.equals(data, readData));

        vm.commit(xid);
    }

    @Test
    public void testReadCommittedIsolation() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);

        long xid2 = vm.begin(0);
        byte[] readData = vm.read(xid2, uid);
        Assert.assertNull("Uncommitted data should not be visible", readData);

        vm.commit(xid1);

        readData = vm.read(xid2, uid);
        Assert.assertNotNull("Committed data should be visible", readData);
        Assert.assertTrue("Read data should match inserted data", Arrays.equals(data, readData));

        vm.commit(xid2);
    }

    @Test
    public void testRepeatableReadIsolation() throws Exception {
        long xid1 = vm.begin(1);
        byte[] data1 = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data1);

        long xid2 = vm.begin(1);
        byte[] readData = vm.read(xid2, uid);
        Assert.assertNull("Uncommitted data should not be visible", readData);

        vm.commit(xid1);

        readData = vm.read(xid2, uid);
        Assert.assertNull("Committed data should not be visible in RR snapshot", readData);

        vm.commit(xid2);
    }

    @Test
    public void testDelete() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);
        vm.commit(xid1);

        long xid2 = vm.begin(0);
        byte[] readData = vm.read(xid2, uid);
        Assert.assertNotNull("Data should be visible before delete", readData);

        boolean deleted = vm.delete(xid2, uid);
        Assert.assertTrue("Delete should succeed", deleted);

        readData = vm.read(xid2, uid);
        Assert.assertNull("Data should not be visible after delete", readData);

        vm.commit(xid2);
    }

    @Test
    public void testAbort() throws Exception {
        long xid = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid, data);

        vm.abort(xid);

        long xid2 = vm.begin(0);
        byte[] readData = vm.read(xid2, uid);
        Assert.assertNull("Aborted transaction data should not be visible", readData);

        vm.commit(xid2);
    }

    @Test
    public void testConcurrentInsert() throws Exception {
        final int threadCount = 2;
        final int insertCount = 10;
        CountDownLatch cdl = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    long xid = vm.begin(0);
                    for (int j = 0; j < insertCount; j++) {
                        byte[] data = RandomUtil.randomBytes(50);
                        long uid = vm.insert(xid, data);
                        Assert.assertTrue("UID should be positive", uid > 0);
                    }
                    vm.commit(xid);
                } catch (Exception e) {
                    Panic.panic(e);
                } finally {
                    cdl.countDown();
                }
            }).start();
        }

        cdl.await();
    }

    @Test
    public void testConcurrentRead() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);
        vm.commit(xid1);

        final int threadCount = 10;
        final int readCount = 100;
        CountDownLatch cdl = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    long xid = vm.begin(0);
                    for (int j = 0; j < readCount; j++) {
                        byte[] readData = vm.read(xid, uid);
                        Assert.assertNotNull("Read data should not be null", readData);
                        Assert.assertTrue("Read data should match inserted data", Arrays.equals(data, readData));
                    }
                    vm.commit(xid);
                } catch (Exception e) {
                    Panic.panic(e);
                } finally {
                    cdl.countDown();
                }
            }).start();
        }

        cdl.await();
    }

    @Test
    public void testConcurrentUpdate() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);
        vm.commit(xid1);

        final int threadCount = 5;
        CountDownLatch cdl = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    long xid = vm.begin(0);
                    byte[] readData = vm.read(xid, uid);
                    if (readData != null) {
                        boolean deleted = vm.delete(xid, uid);
                        if (deleted) {
                            vm.commit(xid);
                        } else {
                            vm.abort(xid);
                        }
                    }
                } catch (Exception e) {
                } finally {
                    cdl.countDown();
                }
            }).start();
        }

        cdl.await();
    }

    @Test
    public void testMultipleTransactions() throws Exception {
        final int transactionCount = 20;
        final int operationCount = 50;
        CountDownLatch cdl = new CountDownLatch(transactionCount);

        for (int i = 0; i < transactionCount; i++) {
            new Thread(() -> {
                try {
                    long xid = vm.begin(0);
                    for (int j = 0; j < operationCount; j++) {
                        if (Math.random() < 0.5) {
                            byte[] data = RandomUtil.randomBytes(50);
                            long uid = vm.insert(xid, data);
                            Assert.assertTrue("UID should be positive", uid > 0);
                        } else {
                            byte[] data = RandomUtil.randomBytes(50);
                            long uid = vm.insert(xid, data);
                            if (uid > 0) {
                                byte[] readData = vm.read(xid, uid);
                                Assert.assertNotNull("Read data should not be null", readData);
                            }
                        }
                    }
                    vm.commit(xid);
                } catch (Exception e) {
                    Panic.panic(e);
                } finally {
                    cdl.countDown();
                }
            }).start();
        }

        cdl.await();
    }

    @Test
    public void testTransactionIsolationLevels() throws Exception {
        long xid1 = vm.begin(1);
        byte[] data1 = RandomUtil.randomBytes(100);
        long uid1 = vm.insert(xid1, data1);

        long xid2 = vm.begin(0);
        byte[] data2 = RandomUtil.randomBytes(100);
        long uid2 = vm.insert(xid2, data2);

        byte[] readData1 = vm.read(xid2, uid1);
        Assert.assertNull("RC should not see uncommitted data", readData1);

        byte[] readData2 = vm.read(xid1, uid2);
        Assert.assertNull("RR should not see uncommitted data", readData2);

        vm.commit(xid1);

        readData1 = vm.read(xid2, uid1);
        Assert.assertNotNull("RC should see committed data", readData1);
        Assert.assertTrue("Read data should match", Arrays.equals(data1, readData1));

        vm.commit(xid2);
    }

    @Test
    public void testSelfVisibility() throws Exception {
        long xid = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid, data);

        byte[] readData = vm.read(xid, uid);
        Assert.assertNotNull("Transaction should see its own uncommitted data", readData);
        Assert.assertTrue("Read data should match inserted data", Arrays.equals(data, readData));

        vm.commit(xid);
    }

    @Test
    public void testUpdateVisibility() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data1 = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data1);
        vm.commit(xid1);

        long xid2 = vm.begin(0);
        boolean deleted = vm.delete(xid2, uid);
        Assert.assertTrue("Delete should succeed", deleted);

        long xid3 = vm.begin(0);
        byte[] readData = vm.read(xid3, uid);
        Assert.assertNotNull("Other transaction should see committed data", readData);

        vm.commit(xid2);

        readData = vm.read(xid3, uid);
        Assert.assertNull("Data should not be visible after delete commit", readData);

        vm.commit(xid3);
    }

    @Test
    public void testLargeData() throws Exception {
        long xid = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(1000);
        long uid = vm.insert(xid, data);

        Assert.assertTrue("UID should be positive", uid > 0);

        byte[] readData = vm.read(xid, uid);
        Assert.assertNotNull("Read data should not be null", readData);
        Assert.assertTrue("Read data should match inserted data", Arrays.equals(data, readData));

        vm.commit(xid);
    }

    @Test
    public void testEmptyData() throws Exception {
        long xid = vm.begin(0);
        byte[] data = new byte[0];
        long uid = vm.insert(xid, data);

        Assert.assertTrue("UID should be positive", uid > 0);

        byte[] readData = vm.read(xid, uid);
        Assert.assertNotNull("Read data should not be null", readData);
        Assert.assertEquals("Read data length should be 0", 0, readData.length);

        vm.commit(xid);
    }
}
