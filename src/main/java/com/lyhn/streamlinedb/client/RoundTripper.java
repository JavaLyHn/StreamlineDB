package com.lyhn.streamlinedb.client;

import com.lyhn.streamlinedb.transport.Packager;
import com.lyhn.streamlinedb.transport.Package;
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        // 接受服务器响应
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
