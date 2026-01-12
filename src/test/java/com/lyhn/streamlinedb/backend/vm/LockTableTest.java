package com.lyhn.streamlinedb.backend.vm;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.utils.Panic;
import org.junit.Assert;
import org.junit.Test;


public class LockTableTest {
    public LockTableTest() {
        Panic.setTestMode(true);
    }

    @Test
    public void testBasicLock() {
        LockTable lt = new LockTable();
        try {
            Lock lock = lt.add(1, 1);
            Assert.assertNull("First lock should be acquired immediately", lock);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testLockConflict() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
            Lock lock = lt.add(2, 1);
            Assert.assertNotNull("Second lock should wait", lock);
            lock.lock();
            lock.unlock();
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testMultipleLocksSameTransaction() {
        LockTable lt = new LockTable();
        try {
            Lock lock1 = lt.add(1, 1);
            Lock lock2 = lt.add(1, 2);
            Lock lock3 = lt.add(1, 3);

            Assert.assertNull("First lock should be acquired immediately", lock1);
            Assert.assertNull("Second lock should be acquired immediately", lock2);
            Assert.assertNull("Third lock should be acquired immediately", lock3);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testLockRelease() {
        LockTable lt = new LockTable();
        try {
            Lock lock = lt.add(1, 1);
            Assert.assertNull("First lock should be acquired immediately", lock);

            lt.remove(1);

            lock = lt.add(2, 1);
            Assert.assertNull("Lock should be acquired after release", lock);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testDeadlockDetection() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
            lt.add(2, 2);
            lt.add(2, 1);

            Assert.assertThrows("Deadlock should be detected", RuntimeException.class, () -> lt.add(1, 2));
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testDeadlockDetectionComplex() {
        LockTable lt = new LockTable();
        for (long i = 1; i <= 100; i++) {
            try {
                Lock lock = lt.add(i, i);
                if (lock != null) {
                    lock.lock();
                    lock.unlock();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        for (long i = 1; i <= 99; i++) {
            try {
                Lock lock = lt.add(i, i + 1);
                if (lock != null) {
                    lock.lock();
                    lock.unlock();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        Assert.assertThrows("Deadlock should be detected", Exception.class, () -> lt.add(100, 1));

        lt.remove(23);

        try {
            Lock lock = lt.add(100, 1);
            if (lock != null) {
                lock.lock();
                lock.unlock();
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testConcurrentLockAcquisition() throws Exception {
        final int threadCount = 10;
        final int lockCount = 100;
        LockTable lt = new LockTable();
        CountDownLatch cdl = new CountDownLatch(threadCount);
        final int[] deadlockCount = new int[1];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    for (long j = 0; j < lockCount; j++) {
                        try {
                            Lock lock = lt.add(threadId, j);
                            if (lock != null) {
                                lock.lock();
                                lock.unlock();
                            }
                        } catch (Exception e) {
                            if (e instanceof com.lyhn.streamlinedb.backend.common.Error.DeadlockException) {
                                synchronized (deadlockCount) {
                                    deadlockCount[0]++;  // 统计死锁次数
                                }
                            } else {
                                throw e;  // 其他异常继续抛出
                            }
                        }
                    }
                    lt.remove(threadId);
                } catch (Exception e) {
                    Panic.panic(e);
                } finally {
                    cdl.countDown();
                }
            }).start();
        }

        cdl.await();

        System.out.println("Deadlock detected count: " + deadlockCount[0]);
    }

    @Test
    public void testLockTableWithWaitQueue() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
            Lock lock2 = lt.add(2, 1);
            Lock lock3 = lt.add(3, 1);

            Assert.assertNotNull("Second lock should wait", lock2);
            Assert.assertNotNull("Third lock should wait", lock3);

            lt.remove(1);

            lock2.lock();
            lock2.unlock();

            lt.remove(2);

            lock3.lock();
            lock3.unlock();

            lt.remove(3);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testMultipleResources() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
            lt.add(1, 2);
            lt.add(1, 3);

            Lock lock2 = lt.add(2, 1);
            Lock lock3 = lt.add(3, 2);
            Lock lock4 = lt.add(4, 3);

            Assert.assertNotNull("Lock should wait", lock2);
            Assert.assertNotNull("Lock should wait", lock3);
            Assert.assertNotNull("Lock should wait", lock4);

            lt.remove(1);

            lock2.lock();
            lock2.unlock();
            lt.remove(2);

            lock3.lock();
            lock3.unlock();
            lt.remove(3);

            lock4.lock();
            lock4.unlock();
            lt.remove(4);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testLockReuse() {
        LockTable lt = new LockTable();
        try {
            Lock lock1 = lt.add(1, 1);
            Assert.assertNull("First lock should be acquired immediately", lock1);

            Lock lock2 = lt.add(1, 1);
            Assert.assertNull("Same transaction should not wait for same resource", lock2);

            lt.remove(1);

            Lock lock3 = lt.add(2, 1);
            Assert.assertNull("Lock should be acquired after release", lock3);

            lt.remove(2);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testCircularWaitDetection() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
            lt.add(2, 2);
            lt.add(3, 3);

            Lock lock1 = lt.add(1, 2);
            Lock lock2 = lt.add(2, 3);

            Assert.assertNotNull("Lock should wait", lock1);
            Assert.assertNotNull("Lock should wait", lock2);

            Assert.assertThrows("Circular wait should be detected", Exception.class, () -> lt.add(3, 1));
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testHighConcurrency() throws Exception {
        final int threadCount = 20;
        final int operationCount = 50;
        final LockTable lt = new LockTable();
        final CountDownLatch cdl = new CountDownLatch(threadCount);
        final int[] deadlockCount = new int[1];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    for (long j = 0; j < operationCount; j++) {
                        long resource = (threadId + j) % 10;
                        try {
                            Lock lock = lt.add(threadId, resource);
                            if (lock != null) {
                                lock.lock();
                                lock.unlock();
                            }
                        } catch (Exception e) {
                            if (e instanceof Error.DeadlockException) {
                                synchronized (deadlockCount) {
                                    deadlockCount[0]++;
                                }
                            } else {
                                throw e;
                            }
                        }
                    }
                    lt.remove(threadId);
                } catch (Exception e) {
                    Panic.panic(e);
                } finally {
                    cdl.countDown();
                }
            }).start();
        }

        cdl.await();

        System.out.println("Deadlock detected count: " + deadlockCount[0]);
    }
    @Test
    public void testLockOrdering() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
            lt.add(2, 2);
            lt.add(3, 3);

            Lock lock1 = lt.add(4, 1);
            Lock lock2 = lt.add(5, 2);
            Lock lock3 = lt.add(6, 3);

            Assert.assertNotNull("Lock should wait", lock1);
            Assert.assertNotNull("Lock should wait", lock2);
            Assert.assertNotNull("Lock should wait", lock3);

            lt.remove(1);

            lock1.lock();
            lock1.unlock();
            lt.remove(4);

            lt.remove(2);

            lock2.lock();
            lock2.unlock();
            lt.remove(5);

            lt.remove(3);

            lock3.lock();
            lock3.unlock();
            lt.remove(6);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    @Test
    public void testLockWithTimeout() throws Exception {
        final LockTable lt = new LockTable();
        CountDownLatch cdl = new CountDownLatch(2);

        try {
            lt.add(1, 1);

            Thread t1 = new Thread(() -> {
                try {
                    Lock lock = lt.add(2, 1);
                    Assert.assertNotNull("Lock should wait", lock);
                    lock.lock();
                    lock.unlock();
                    lt.remove(2);
                } catch (Exception e) {
                    Panic.panic(e);
                } finally {
                    cdl.countDown();
                }
            });

            Thread t2 = new Thread(() -> {
                try {
                    Thread.sleep(100);
                    lt.remove(1);
                } catch (Exception e) {
                    Panic.panic(e);
                } finally {
                    cdl.countDown();
                }
            });

            t1.start();
            t2.start();

            cdl.await();
        } catch (Exception e) {
            Panic.panic(e);
        }
    }
}
