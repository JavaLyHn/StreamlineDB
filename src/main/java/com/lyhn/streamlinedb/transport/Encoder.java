package com.lyhn.streamlinedb.transport;

import com.google.common.primitives.Bytes;

import java.util.Arrays;

// 编解码
public class Encoder {
    public byte[] encode(Package pkg){
        if(pkg.getError() != null){
            Exception error = pkg.getError();
            String msg = "Intern server error!";
            if(error.getMessage() != null){
                msg = error.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        }else{
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    public Package decode(byte[] data) throws Exception {
        if(data.length < 1){
            throw new Exception("Invalid package data!");
        }
        // 无异常
        if(data[0] == 0){
            return new Package(Arrays.copyOfRange(data,1,data.length),null);
        }else if(data[1] == 1){
            return new Package(null,new RuntimeException(new String(Arrays.copyOfRange(data,1,data.length))));
        }else{
            throw new Exception("Invalid package data!");
        }
    }
}
