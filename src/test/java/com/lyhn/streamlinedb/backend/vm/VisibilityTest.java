package com.lyhn.streamlinedb.backend.vm;


import java.io.File;
import java.util.Arrays;

import com.lyhn.streamlinedb.backend.dm.DataManager;
import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;
import com.lyhn.streamlinedb.backend.tm.TransactionManager;
import com.lyhn.streamlinedb.backend.utils.Panic;
import com.lyhn.streamlinedb.backend.utils.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class VisibilityTest {
    private static final String TEST_PATH = "E:\\StreamlineDB\\visibility_test";
    private TransactionManager tm;
    private DataManager dm;
    private VersionManager vm;

    public VisibilityTest() {
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
    public void testReadCommittedUncommittedData() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);

        long xid2 = vm.begin(0);
        byte[] readData = vm.read(xid2, uid);

        Assert.assertNull("Read Committed should not see uncommitted data", readData);

        vm.commit(xid1);
        vm.commit(xid2);
    }

    @Test
    public void testReadCommittedCommittedData() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);
        vm.commit(xid1);

        long xid2 = vm.begin(0);
        byte[] readData = vm.read(xid2, uid);

        Assert.assertNotNull("Read Committed should see committed data", readData);
        Assert.assertTrue("Read data should match inserted data", Arrays.equals(data, readData));

        vm.commit(xid2);
    }

    @Test
    public void testReadCommittedDeletedData() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);
        vm.commit(xid1);

        long xid2 = vm.begin(0);
        boolean deleted = vm.delete(xid2, uid);
        Assert.assertTrue("Delete should succeed", deleted);

        long xid3 = vm.begin(0);
        byte[] readData = vm.read(xid3, uid);
        Assert.assertNotNull("Read Committed should see committed data before delete", readData);

        vm.commit(xid2);

        readData = vm.read(xid3, uid);
        Assert.assertNull("Read Committed should not see deleted data", readData);

        vm.commit(xid3);
    }

    @Test
    public void testRepeatableReadUncommittedData() throws Exception {
        long xid1 = vm.begin(1);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);

        long xid2 = vm.begin(1);
        byte[] readData = vm.read(xid2, uid);

        Assert.assertNull("Repeatable Read should not see uncommitted data", readData);

        vm.commit(xid1);
        vm.commit(xid2);
    }

    @Test
    public void testRepeatableReadCommittedData() throws Exception {
        long xid1 = vm.begin(1);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);
        vm.commit(xid1);

        long xid2 = vm.begin(1);
        byte[] readData = vm.read(xid2, uid);

        Assert.assertNotNull("Repeatable Read should see data committed before snapshot", readData);
        Assert.assertTrue("Read data should match", Arrays.equals(data, readData));

        vm.commit(xid2);
    }

    @Test
    public void testRepeatableReadSnapshotConsistency() throws Exception {
        long xid1 = vm.begin(1);
        byte[] data1 = RandomUtil.randomBytes(100);
        long uid1 = vm.insert(xid1, data1);
        vm.commit(xid1);

        long xid2 = vm.begin(1);
        byte[] readData1 = vm.read(xid2, uid1);
        Assert.assertNotNull("Repeatable Read should see data committed before snapshot", readData1);
        Assert.assertTrue("Read data should match", Arrays.equals(data1, readData1));

        long xid3 = vm.begin(1);
        byte[] data2 = RandomUtil.randomBytes(100);
        long uid2 = vm.insert(xid3, data2);
        vm.commit(xid3);

        byte[] readData2 = vm.read(xid2, uid2);
        Assert.assertNull("Repeatable Read should not see data committed after snapshot", readData2);

        vm.commit(xid2);
    }

    @Test
    public void testSelfVisibilityUncommitted() throws Exception {
        long xid = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid, data);

        byte[] readData = vm.read(xid, uid);

        Assert.assertNotNull("Transaction should see its own uncommitted data", readData);
        Assert.assertTrue("Read data should match inserted data", Arrays.equals(data, readData));

        vm.commit(xid);
    }

    @Test
    public void testSelfVisibilityDeleted() throws Exception {
        long xid = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid, data);

        boolean deleted = vm.delete(xid, uid);
        Assert.assertTrue("Delete should succeed", deleted);

        byte[] readData = vm.read(xid, uid);
        Assert.assertNull("Transaction should not see its own deleted data", readData);

        vm.commit(xid);
    }

    @Test
    public void testMultipleTransactionsVisibility() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data1 = RandomUtil.randomBytes(100);
        long uid1 = vm.insert(xid1, data1);

        long xid2 = vm.begin(0);
        byte[] data2 = RandomUtil.randomBytes(100);
        long uid2 = vm.insert(xid2, data2);

        byte[] readData1 = vm.read(xid1, uid2);
        Assert.assertNull("Transaction should not see other's uncommitted data", readData1);

        byte[] readData2 = vm.read(xid2, uid1);
        Assert.assertNull("Transaction should not see other's uncommitted data", readData2);

        vm.commit(xid1);

        readData2 = vm.read(xid2, uid1);
        Assert.assertNotNull("Transaction should see other's committed data", readData2);
        Assert.assertTrue("Read data should match", Arrays.equals(data1, readData2));

        vm.commit(xid2);
    }

    @Test
    public void testUpdateVisibility() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data1 = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data1);
        vm.commit(xid1);

        long xid2 = vm.begin(0);
        byte[] readData1 = vm.read(xid2, uid);
        Assert.assertNotNull("Transaction should see committed data", readData1);
        Assert.assertTrue("Read data should match", Arrays.equals(data1, readData1));

        boolean deleted = vm.delete(xid2, uid);
        Assert.assertTrue("Delete should succeed", deleted);

        byte[] readData2 = vm.read(xid2, uid);
        Assert.assertNull("Transaction should not see its own deleted data", readData2);

        long xid3 = vm.begin(0);
        byte[] readData3 = vm.read(xid3, uid);
        Assert.assertNotNull("Other transaction should see committed data", readData3);
        Assert.assertTrue("Read data should match", Arrays.equals(data1, readData3));

        vm.commit(xid2);

        readData3 = vm.read(xid3, uid);
        Assert.assertNull("Other transaction should not see deleted data", readData3);

        vm.commit(xid3);
    }

    @Test
    public void testAbortedTransactionVisibility() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);

        vm.abort(xid1);

        long xid2 = vm.begin(0);
        byte[] readData = vm.read(xid2, uid);
        Assert.assertNull("Aborted transaction data should not be visible", readData);

        vm.commit(xid2);
    }

    @Test
    public void testConcurrentVisibility() throws Exception {
        final int threadCount = 5;
        final int insertCount = 10;
        final long[][] uids = new long[threadCount][insertCount];

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    long xid = vm.begin(0);
                    for (int j = 0; j < insertCount; j++) {
                        byte[] data = RandomUtil.randomBytes(50);
                        uids[threadId][j] = vm.insert(xid, data);
                    }
                    vm.commit(xid);
                } catch (Exception e) {
                    Panic.panic(e);
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        long xid = vm.begin(0);
        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < insertCount; j++) {
                byte[] readData = vm.read(xid, uids[i][j]);
                Assert.assertNotNull("Should see all committed data", readData);
            }
        }
        vm.commit(xid);
    }

    @Test
    public void testVisibilityWithMultipleVersions() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data1 = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data1);
        vm.commit(xid1);

        long xid2 = vm.begin(0);
        byte[] readData1 = vm.read(xid2, uid);
        Assert.assertNotNull("Should see first version", readData1);
        Assert.assertTrue("Data should match first version", Arrays.equals(data1, readData1));

        boolean deleted = vm.delete(xid2, uid);
        Assert.assertTrue("Delete should succeed", deleted);

        long xid3 = vm.begin(0);
        byte[] readData2 = vm.read(xid3, uid);
        Assert.assertNotNull("Should see committed version", readData2);
        Assert.assertTrue("Data should match first version", Arrays.equals(data1, readData2));

        vm.commit(xid2);

        readData2 = vm.read(xid3, uid);
        Assert.assertNull("Should not see deleted version", readData2);

        vm.commit(xid3);
    }

    @Test
    public void testVisibilityIsolation() throws Exception {
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
        Assert.assertTrue("Data should match", Arrays.equals(data1, readData1));

        vm.commit(xid2);
    }

    @Test
    public void testVisibilityWithRollback() throws Exception {
        long xid1 = vm.begin(0);
        byte[] data = RandomUtil.randomBytes(100);
        long uid = vm.insert(xid1, data);

        vm.abort(xid1);

        long xid2 = vm.begin(0);
        byte[] readData = vm.read(xid2, uid);
        Assert.assertNull("Should not see aborted data", readData);

        byte[] newData = RandomUtil.randomBytes(100);
        long newUid = vm.insert(xid2, newData);
        Assert.assertTrue("Should be able to insert new data", newUid > 0);

        byte[] readNewData = vm.read(xid2, newUid);
        Assert.assertNotNull("Should see own uncommitted data", readNewData);
        Assert.assertTrue("Data should match", Arrays.equals(newData, readNewData));

        vm.commit(xid2);
    }
}
