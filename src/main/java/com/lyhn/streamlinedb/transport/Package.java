package com.lyhn.streamlinedb.transport;

// 数据包对象
public class Package {
    byte[] data;
    Exception error;

    public Package(byte[] data,Exception error){
        this.data = data;
        this.error = error;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getError() {
        return error;
    }
}
