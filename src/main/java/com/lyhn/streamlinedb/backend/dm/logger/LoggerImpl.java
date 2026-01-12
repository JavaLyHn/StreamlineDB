package com.lyhn.streamlinedb.backend.dm.logger;

import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.dm.page.Page;
import com.lyhn.streamlinedb.backend.utils.Panic;
import com.lyhn.streamlinedb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LoggerImpl implements Logger{
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel channel;
    private Lock lock;

    private long position;// 当前日志指针位置
    private long fileSize;

    private int xCheckSum;

    LoggerImpl(RandomAccessFile file, FileChannel channel) {
        this.file = file;
        this.channel = channel;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile file, FileChannel channel,int xCheckSum) {
        this.file = file;
        this.channel = channel;
        this.xCheckSum = xCheckSum;
        lock = new ReentrantLock();
    }

    // 初始化日志文件、读取文件元数据、验证文件完整性
    void init(){
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4){
            Panic.panic(Error.badLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try{
            channel.position(0);
            channel.read(raw);
        }catch (Exception e){
            Panic.panic(e);
        }
        int xCheckSum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xCheckSum = xCheckSum;

        checkAndRemoveTail();
    }

    // 检查日志完整性并且自动移除损坏的日志尾部
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while(true){
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xCheckSum){
            Panic.panic(Error.badLogFileException);
        }
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    private byte[] internNext() {
        if(position + OF_DATA >= fileSize){
            return null;
        }
        ByteBuffer tmp =  ByteBuffer.allocate(4);
        try{
            channel.position(position);
            // 读取size字段
            channel.read(tmp);
        }catch (Exception e){
            Panic.panic(e);
        }

        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize){
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            channel.position(position);
            channel.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        // 实时计算的校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 日志文件中存储的校验和
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    // 将数据写入日志文件
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            channel.position(channel.size());
            channel.write(buf);
            fileSize = channel.size();
            updateXCheckSum(log);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            lock.unlock();
        }

    }

    private void updateXCheckSum(byte[] log) {
        this.xCheckSum = calChecksum(this.xCheckSum,log);
        try {
            channel.position(0);
            channel.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
            channel.force(false);
        }catch (Exception e){
            Panic.panic(e);
        }
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            // 删除该位置之后的所有数据
            channel.truncate(x);
            fileSize = x;
        }finally {
            lock.unlock();
        }
    }

    private byte[] wrapLog(byte[] data){
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return concat(size, checksum, data);
    }

    private byte[] concat(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] array : arrays) {
            totalLength += array.length;
        }
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array,0,result,offset,array.length);
            offset += array.length;
        }
        return result;
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            channel.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
}
