package com.lyhn.streamlinedb.backend.server;

import com.lyhn.streamlinedb.backend.tbm.TableManager;
import com.lyhn.streamlinedb.transport.Encoder;
import com.lyhn.streamlinedb.transport.Packager;
import com.lyhn.streamlinedb.transport.Package;
import com.lyhn.streamlinedb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Server {
    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("StreamlineDB Server listen to port: " + port);
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while(true) {
                Socket socket = ss.accept();
                // 接受客户端连接
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {}
        }
    }

    class HandleSocket implements Runnable {
        private Socket socket;
        private TableManager tbm;

        public HandleSocket(Socket socket, TableManager tbm) {
            this.socket = socket;
            this.tbm = tbm;
        }

        @Override
        public void run() {
            InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
            System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());
            Packager packager = null;
            try {
                // 初始化
                Transporter t = new Transporter(socket);
                Encoder e = new Encoder();
                packager = new Packager(t, e);
            } catch(IOException e) {
                e.printStackTrace();
                try {
                    socket.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                return;
            }
            Executor exe = new Executor(tbm);
            while(true) {
                Package pkg = null;
                try {
                    // 接收客户端请求
                    pkg = packager.receive();
                } catch(Exception e) {
                    break;
                }
                byte[] sql = pkg.getData();
                byte[] result = null;
                Exception e = null;
                try {
                    result = exe.execute(sql);
                } catch (Exception e1) {
                    e = e1;
                    e.printStackTrace();
                }
                pkg = new Package(result, e);
                try {
                    // 发送响应给客户端
                    packager.send(pkg);
                } catch (Exception e1) {
                    e1.printStackTrace();
                    break;
                }
            }
            exe.close();
            try {
                packager.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
