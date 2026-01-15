package com.lyhn.streamlinedb.client;

import com.lyhn.streamlinedb.transport.Packager;
import com.lyhn.streamlinedb.transport.Package;
public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        // 与服务器通信
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getError() != null) {
            throw resPkg.getError();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }
}
