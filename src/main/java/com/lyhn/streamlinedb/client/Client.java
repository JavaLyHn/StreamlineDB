package com.lyhn.streamlinedb.client;

import com.lyhn.streamlinedb.transport.Encoder;
import com.lyhn.streamlinedb.transport.Package;
import com.lyhn.streamlinedb.transport.Packager;
import com.lyhn.streamlinedb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

// 客户端
public class Client {
    private Packager packager;

    public Client(Packager packager){
        this.packager = packager;
    }

    public void run(){
        Scanner scanner = new Scanner(System.in);
        System.out.println("Connected to server. Type 'exit' to quit.");
        while(true){
            System.out.println("> ");
            String input = scanner.nextLine();
            if("exit".equalsIgnoreCase(input)){
                break;
            }

            try {
                Package request = new Package(input.getBytes(), null);
                packager.send(request);

                Package response = packager.receive();
                if(response.getError() != null){
                    System.out.println(response.getError().getMessage());
                }else{
                    System.out.println(new  String(response.getData()));
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
                break;
            }
        }
        try{
            packager.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);
        new Client(packager).run();
    }
}
