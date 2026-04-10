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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Server {
    private int port;
    TableManager tbm;
    private ServerSocket ss;
    private volatile boolean running = true;
    private AtomicInteger activeConnections = new AtomicInteger(0);
    private static final long IDLE_SHUTDOWN_SECONDS = 10;
    private long lastActiveTimeMillis = System.currentTimeMillis();
    private ScheduledExecutorService idleChecker;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("StreamlineDB Server listen to port: " + port);
        System.out.println("[Server] Auto-shutdown enabled: will exit after " + IDLE_SHUTDOWN_SECONDS + "s of zero connections");

        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());

        startIdleMonitor();

        try {
            while(running && !ss.isClosed()) {
                Socket socket = ss.accept();
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            }
        } catch(IOException e) {
            if(running) e.printStackTrace();
        } finally {
            stopIdleMonitor();
            tpe.shutdown();
            try { if(ss != null && !ss.isClosed()) ss.close(); } catch (IOException ignored) {}
            System.out.println("[Server] Server stopped.");
            System.exit(0);
        }
    }

    private void startIdleMonitor() {
        idleChecker = Executors.newSingleThreadScheduledExecutor();
        idleChecker.scheduleAtFixedRate(() -> {
            int connCount = activeConnections.get();
            if(connCount == 0) {
                long idleTime = System.currentTimeMillis() - lastActiveTimeMillis;
                if(idleTime >= IDLE_SHUTDOWN_SECONDS * 1000) {
                    System.out.println("[Server] No clients connected for " + IDLE_SHUTDOWN_SECONDS + "s, shutting down...");
                    running = false;
                    try { if(ss != null && !ss.isClosed()) ss.close(); } catch (IOException ignored) {}
                    stopIdleMonitor();
                } else {
                    long remaining = IDLE_SHUTDOWN_SECONDS - idleTime / 1000;
                    System.out.println("[Server] Idle: " + remaining + "s until auto-shutdown (connections: 0)");
                }
            } else {
                lastActiveTimeMillis = System.currentTimeMillis();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void stopIdleMonitor() {
        if(idleChecker != null && !idleChecker.isShutdown()) {
            idleChecker.shutdown();
            try { if(!idleChecker.awaitTermination(2, TimeUnit.SECONDS)) idleChecker.shutdownNow(); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }

    void onConnect(String address) {
        int count = activeConnections.incrementAndGet();
        lastActiveTimeMillis = System.currentTimeMillis();
        System.out.println("Establish connection: " + address + " [total: " + count + "]");
    }

    void onDisconnect(String address) {
        int count = activeConnections.decrementAndGet();
        if(count == 0) {
            lastActiveTimeMillis = System.currentTimeMillis();
        }
        System.out.println("Connection closed: " + address + " [remaining: " + count + "]");
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
            String clientInfo = address.getAddress().getHostAddress()+":"+address.getPort();
            onConnect(clientInfo);

            Packager packager = null;
            try {
                Transporter t = new Transporter(socket);
                Encoder e = new Encoder();
                packager = new Packager(t, e);
            } catch(IOException e) {
                e.printStackTrace();
                safeClose(socket);
                onDisconnect(clientInfo);
                return;
            }
            Executor exe = new Executor(tbm);
            while(running) {
                Package pkg = null;
                try {
                    pkg = packager.receive();
                } catch(Exception e) {
                    break;
                }
                byte[] sql = pkg.getData();
                byte[] result = null;
                Exception err = null;
                try {
                    result = exe.execute(sql);
                } catch (Exception e1) {
                    err = e1;
                    e1.printStackTrace();
                }
                pkg = new Package(result, err);
                try {
                    packager.send(pkg);
                } catch (Exception e1) {
                    e1.printStackTrace();
                    break;
                }
            }
            exe.close();
            try {
                if(packager != null) packager.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            onDisconnect(clientInfo);
        }

        private void safeClose(Socket s) {
            try { s.close(); } catch (IOException ignored) {}
        }
    }
}
