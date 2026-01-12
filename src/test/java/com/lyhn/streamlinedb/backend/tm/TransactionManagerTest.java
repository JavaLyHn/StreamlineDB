package com.lyhn.streamlinedb.backend.tm;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static org.junit.Assert.*;
public class TransactionManagerTest {
    static Random random = new SecureRandom();
    private static final String TEST_PATH = "E:\\StreamlineDB\\";

    private int transCnt = 0;
    private int noWorkers = 50;
    private int noWorks = 3000;
    private Lock lock = new ReentrantLock();
    private TransactionManager tmger;
    private Map<Long, Byte> transMap;
    private CountDownLatch cdl;

    @Before
    public void setUp() {
        tmger = TransactionManager.create(TEST_PATH);
        transMap = new HashMap<>();
        cdl = new CountDownLatch(noWorkers);
    }

    @After
    public void tearDown() {
        tmger.close();
        new File(TEST_PATH + ".xid").delete();
    }

    @Test
    public void testSingleTransaction() {
        long xid = tmger.begin();
        assertTrue(tmger.isActive(xid));
        assertFalse(tmger.isCommitted(xid));
        assertFalse(tmger.isAborted(xid));

        tmger.commit(xid);
        assertFalse(tmger.isActive(xid));
        assertTrue(tmger.isCommitted(xid));
        assertFalse(tmger.isAborted(xid));
    }

    @Test
    public void testTransactionAbort() {
        long xid = tmger.begin();
        assertTrue(tmger.isActive(xid));

        tmger.abort(xid);
        assertFalse(tmger.isActive(xid));
        assertFalse(tmger.isCommitted(xid));
        assertTrue(tmger.isAborted(xid));
    }

    @Test
    public void testMultipleTransactions() {
        List<Long> xids = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            long xid = tmger.begin();
            xids.add(xid);
            assertTrue(tmger.isActive(xid));
        }

        for (int i = 0; i < 5; i++) {
            tmger.commit(xids.get(i));
            assertTrue(tmger.isCommitted(xids.get(i)));
        }

        for (int i = 5; i < 10; i++) {
            tmger.abort(xids.get(i));
            assertTrue(tmger.isAborted(xids.get(i)));
        }
    }

    @Test
    public void testSuperXID() {
        assertEquals(TransactionManagerImpl.SUPER_XID, 0);
        assertFalse(tmger.isActive(TransactionManagerImpl.SUPER_XID));
        assertTrue(tmger.isCommitted(TransactionManagerImpl.SUPER_XID));
        assertFalse(tmger.isAborted(TransactionManagerImpl.SUPER_XID));
    }

    @Test
    public void testTransactionTimeout() throws InterruptedException {
        long timeout = 2000;
        TransactionManager tm = TransactionManager.create(TEST_PATH + "_timeout", timeout);

        long xid = tm.begin();
        assertTrue(tm.isActive(xid));

        Thread.sleep(2500);

        assertFalse(tm.isActive(xid));
        assertTrue(tm.isAborted(xid));

        tm.close();
        new File(TEST_PATH + "_timeout.xid").delete();
    }

    @Test
    public void testTransactionNoTimeout() throws InterruptedException {
        long timeout = 5000;
        TransactionManager tm = TransactionManager.create(TEST_PATH + "_no_timeout", timeout);

        long xid = tm.begin();
        assertTrue(tm.isActive(xid));

        Thread.sleep(2000);

        assertTrue(tm.isActive(xid));

        tm.commit(xid);
        assertTrue(tm.isCommitted(xid));

        tm.close();
        new File(TEST_PATH + "_no_timeout.xid").delete();
    }

    @Test
    public void testMultiThread() {
        tmger = TransactionManager.create(TEST_PATH + "_multi");
        transMap = new HashMap<>();
        cdl = new CountDownLatch(noWorkers);
        for(int i = 0; i < noWorkers; i ++) {
            Runnable r = () -> worker();
            new Thread(r).run();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tmger.close();
        new File(TEST_PATH + "_multi.xid").delete();
    }

    private void worker() {
        boolean inTrans = false;
        long transXID = 0;
        for(int i = 0; i < noWorks; i ++) {
            int op = Math.abs(random.nextInt(6));
            if(op == 0) {
                lock.lock();
                if(inTrans == false) {
                    long xid = tmger.begin();
                    transMap.put(xid, (byte)0);
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                lock.lock();
                if(transCnt > 0) {
                    long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    Byte status = transMap.get(xid);
                    if(status != null) {
                        boolean ok = false;
                        switch (status) {
                            case 0:
                                ok = tmger.isActive(xid);
                                break;
                            case 1:
                                ok = tmger.isCommitted(xid);
                                break;
                            case 2:
                                ok = tmger.isAborted(xid);
                                break;
                        }
                        assertTrue(ok);
                    }
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }

    @Test
    public void testTransactionPersistence() {
        long xid1 = tmger.begin();
        long xid2 = tmger.begin();

        tmger.commit(xid1);
        tmger.abort(xid2);

        tmger.close();

        TransactionManager tm2 = TransactionManager.open(TEST_PATH);
        assertFalse(tm2.isActive(xid1));
        assertTrue(tm2.isCommitted(xid1));
        assertFalse(tm2.isAborted(xid1));

        assertFalse(tm2.isActive(xid2));
        assertFalse(tm2.isCommitted(xid2));
        assertTrue(tm2.isAborted(xid2));

        tm2.close();
    }
}
