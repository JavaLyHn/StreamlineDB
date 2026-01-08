package com.lyhn.streamlinedb.backend.utils;

public class Panic {
    public static void panic(Exception e){
        e.printStackTrace();
        System.exit(1);
    }
}
