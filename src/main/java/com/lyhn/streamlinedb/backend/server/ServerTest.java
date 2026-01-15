package com.lyhn.streamlinedb.backend.server;

import com.lyhn.streamlinedb.transport.Encoder;
import com.lyhn.streamlinedb.transport.Package;
import com.lyhn.streamlinedb.transport.Packager;
import com.lyhn.streamlinedb.transport.Transporter;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// 服务端
public class ServerTest {
    private int port;
    public ServerTest(int port){
        this.port = port;
    }

    public void start(){
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        System.out.println("Server listen to port: " + port);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                10,
                20,
                1L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        try{
            while(true){
                Socket socket = serverSocket.accept();
                Runnable runnable = new HandlerSocket(socket);
                threadPoolExecutor.execute(runnable);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                serverSocket.close();
            } catch (IOException ignored) {}
        }
    }

    public static void main(String[] args) {
        ServerTest serverTest = new ServerTest(9999);
        serverTest.start();
    }

    class HandlerSocket implements Runnable{
        private Socket socket;

        public HandlerSocket(Socket socket){
            this.socket = socket;
        }

        @Override
        public void run() {
            System.out.println("New connection accepted: " + socket.getInetAddress());
            Packager packager = null;
            try{
                Transporter transporter = new Transporter(socket);
                Encoder encoder = new Encoder();
                packager = new Packager(transporter, encoder);
            }catch (Exception e){
                e.printStackTrace();
                try {
                    socket.close();
                }catch (IOException exception){
                    exception.printStackTrace();
                }
                return;
            }
            while(true){
                Package pkg = null;
                try {
                    pkg = packager.receive();
                } catch (Exception e) {
                    System.out.println("Connection closed");
                    break;
                }
                byte[] data = pkg.getData();
                String message = new String(data);
                System.out.println("Received message: " + message);

                byte[] result = ("Echo: " + message).getBytes();
                pkg = new Package(result, null);
                try {
                    packager.send(pkg);
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            try {
                packager.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
