package com.lyhn.streamlinedb.backend.tm;

import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.utils.Panic;
import com.lyhn.streamlinedb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// 负责管理数据库中的事务生命周期，包括事务的创建、提交、回滚以及状态跟踪。它通过维护一个特殊的事务文件
public class TransactionManagerImpl implements TransactionManager{
    // XID文件头长度，保存最新创建的事务id
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;// 活跃
    private static final byte FIELD_TRAN_COMMITTED = 1;// 提交
    private static final byte FIELD_TRAN_ABORTED  = 2;// 撤销

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";

    // 默认事务超时时间（30秒）
    private static final long DEFAULT_TIMEOUT = 30000;

    // xid 事务文件（主要用于获取文件基本信息）
    private RandomAccessFile file;
    // xid 文件读取（主要负责文件读写操作）
    private FileChannel fc;
    // 当前事务
    private long xidCounter;
    private Lock counterLock;

    // 事务超时相关
    private Map<Long, Long> activeTransactions;
    private long timeout;
    private ScheduledExecutorService timeoutChecker;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        activeTransactions = new ConcurrentHashMap<>();
        timeout = DEFAULT_TIMEOUT;
        checkXIDCounter();
        startTimeoutChecker();
    }

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc, long timeout) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        activeTransactions = new ConcurrentHashMap<>();
        this.timeout = timeout;
        checkXIDCounter();
        startTimeoutChecker();
    }

    // 检查xid文件是否合法
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            // 读取文件物理长度
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.badXIDFileException);
        }

        // 确保文件至少包含文件头（8B），文件过小说明不完整
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.badXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            // 定位到文件头
            fc.position(0);
            // 读取8字节的文件头
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 解析到当前最大的xid编号
        this.xidCounter = Parser.parseLong(buf.array());
        // 计算文件理论长度
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen) {
            Panic.panic(Error.badXIDFileException);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }

    // 启动事务超时检查器
    private void startTimeoutChecker() {
        timeoutChecker = Executors.newSingleThreadScheduledExecutor();
        timeoutChecker.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            activeTransactions.forEach((xid,startTime) -> {
                if(currentTime - startTime > timeout) {
                    try {
                        abort(xid);
                        activeTransactions.remove(xid);
                    } catch (Exception e) {
                        // 忽略超时回滚错误
                    }
                }
            });
        },1,1, TimeUnit.SECONDS);
    }

    // 更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        // 创建1B的缓冲区
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            // 精确定位到事务位置
            fc.position(offset);
            // 将状态字节写入文件
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制刷盘
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        // 当前事务计数器+1（记录下一个可用的事务id）
        xidCounter ++;
        // 将long类型转为8字节数组
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            // 写入文件头
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制刷盘
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }



    @Override
    public long begin() {
        counterLock.lock();
        try {
            // 事务id等于当前计数器+1
            long xid = xidCounter + 1;
            // 更新事务状态活跃
            updateXID(xid, FIELD_TRAN_ACTIVE);
            // 更新事务计数器
            incrXIDCounter();
            // 记录事务开始时间
            activeTransactions.put(xid, System.currentTimeMillis());
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
        activeTransactions.remove(xid);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
        activeTransactions.remove(xid);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        // 根据事务id计算在.xid文件中的位置
        long offset = getXidPosition(xid);
        // 读取1B的事务状态
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 判断读取的状态是否等于目标状态（已提交）
        return buf.array()[0] == status;
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        // 超级事务永远处于已提交状态
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        stopTimeoutChecker();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void stopTimeoutChecker() {
        if(timeoutChecker != null && !timeoutChecker.isShutdown()) {
            timeoutChecker.shutdown();
            try {
                if(!timeoutChecker.awaitTermination(5,TimeUnit.SECONDS)){
                    timeoutChecker.shutdown();
                }
            } catch (InterruptedException e) {
                timeoutChecker.shutdown();
                Thread.currentThread().interrupt();
            }
        }
    }
}
