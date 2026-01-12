package com.lyhn.streamlinedb.backend.utils;

public class Panic {
    private static boolean testMode = false;

    public static void setTestMode(boolean mode) {
        testMode = mode;
    }

    public static boolean isTestMode() {
        return testMode;
    }

    public static void panic(Exception e){
        e.printStackTrace();
        System.exit(1);
    }
}
