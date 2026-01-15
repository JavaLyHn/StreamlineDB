package com.lyhn.streamlinedb.client;

import com.lyhn.streamlinedb.transport.Encoder;
import com.lyhn.streamlinedb.transport.Packager;
import com.lyhn.streamlinedb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

public class Launcher {
    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);

        Client client = new Client(packager);
        Shell shell = new Shell(client);

        // 通过shell输入命令
        shell.run();
    }
}
