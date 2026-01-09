package com.lyhn.streamlinedb.backend.common;

// 错误定义
public class Error {
    public static final CacheFullException cacheFullException = new CacheFullException();
    public static final FailCreateFile failCreateFile = new FailCreateFile();
    public static final NoFilePermission  noFilePermission = new NoFilePermission();
    public static final NoFileExist noFileExist = new NoFileExist();
    public static final BadLogFileException BadLogFileException = new BadLogFileException();


    public static class CacheFullException extends RuntimeException {
        public CacheFullException() {
            super("Cache is full");
        }
    }

    public static class FailCreateFile extends RuntimeException {
        public FailCreateFile() {
            super("Fail CreateFile");
        }
    }

    public static class NoFilePermission extends RuntimeException {
        public NoFilePermission() {
            super("NoFilePermission");
        }
    }

    public static class NoFileExist extends RuntimeException {
        public NoFileExist() {
            super("NoFileExist");
        }
    }

    public static class BadLogFileException extends RuntimeException {
        public BadLogFileException() {
            super("Bad log file");
        }
    }
}
