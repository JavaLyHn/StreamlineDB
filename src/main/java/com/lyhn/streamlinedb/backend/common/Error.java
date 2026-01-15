package com.lyhn.streamlinedb.backend.common;

// 错误定义
public class Error {
    public static final CacheFullException cacheFullException = new CacheFullException();
    public static final FailCreateFile failCreateFile = new FailCreateFile();
    public static final NoFilePermission  noFilePermission = new NoFilePermission();
    public static final NoFileExist noFileExist = new NoFileExist();
    public static final BadLogFileException badLogFileException = new BadLogFileException();
    public static final BadXIDFileException badXIDFileException = new BadXIDFileException();
    public static final DataTooLargeException dataTooLargeException = new DataTooLargeException();
    public static final DatabaseBusyException databaseBusyException = new DatabaseBusyException();
    public static final DeadlockException deadlockException = new DeadlockException();
    public static final NullEntryException nullEntryException = new NullEntryException();
    public static final ConcurrentUpdateException concurrentUpdateException = new ConcurrentUpdateException();
    public static final InvalidCommandException invalidCommandException = new InvalidCommandException();
    public static final TableNoIndexException tableNoIndexException = new TableNoIndexException();
    public static final InvalidFieldException invalidFieldException = new InvalidFieldException();
    public static final FieldNotFoundException fieldNotFoundException = new FieldNotFoundException();
    public static final InvalidLogOpException invalidLogOpException = new InvalidLogOpException();
    public static final InvalidValuesException invalidValuesException = new InvalidValuesException();
    public static final FieldNotIndexedException fieldNotIndexedException = new FieldNotIndexedException();
    public static final DuplicatedTableException duplicatedTableException = new DuplicatedTableException();
    public static final TableNotFoundException tableNotFoundException = new TableNotFoundException();
    public static final InvalidMemException invalidMemException = new InvalidMemException();
    public static final NestedTransactionException nestedTransactionException = new NestedTransactionException();
    public static final NoTransactionException noTransactionException = new NoTransactionException();

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

    public static class BadXIDFileException extends RuntimeException {
        public BadXIDFileException() {
            super("Bad log file");
        }
    }

    public static class DataTooLargeException extends RuntimeException {
        public DataTooLargeException() {
            super("Bad log file");
        }
    }

    public static class DatabaseBusyException extends RuntimeException {
        public DatabaseBusyException() {
            super("Bad log file");
        }
    }

    public static class DeadlockException extends RuntimeException {
        public DeadlockException() {
            super("Bad log file");
        }
    }

    public static class NullEntryException extends RuntimeException {
        public NullEntryException() {
            super("Bad log file");
        }
    }

    public static class ConcurrentUpdateException extends RuntimeException {
        public ConcurrentUpdateException() {
            super("Bad log file");
        }
    }

    public static class InvalidCommandException extends RuntimeException {
        public InvalidCommandException() {
            super("Bad log file");
        }
    }

    public static class TableNoIndexException extends RuntimeException {
        public TableNoIndexException() {
            super("Bad log file");
        }
    }

    public static class InvalidFieldException extends RuntimeException {
        public InvalidFieldException() {
            super("Bad log file");
        }
    }

    public static class FieldNotFoundException extends RuntimeException {
        public FieldNotFoundException() {
            super("Bad log file");
        }
    }

    public static class InvalidLogOpException extends RuntimeException {
        public InvalidLogOpException() {
            super("Bad log file");
        }
    }

    public static class InvalidValuesException extends RuntimeException {
        public InvalidValuesException() {
            super("Bad log file");
        }
    }

    public static class FieldNotIndexedException extends RuntimeException {
        public FieldNotIndexedException() {
            super("Bad log file");
        }
    }

    public static class DuplicatedTableException extends RuntimeException {
        public DuplicatedTableException() {
            super("Bad log file");
        }
    }

    public static class TableNotFoundException extends RuntimeException {
        public TableNotFoundException() {
            super("Bad log file");
        }
    }

    public static class InvalidMemException extends RuntimeException {
        public InvalidMemException() {
            super("Bad log file");
        }
    }

    public static class NestedTransactionException extends RuntimeException {
        public NestedTransactionException() {
            super("Bad log file");
        }
    }

    public static class NoTransactionException extends RuntimeException {
        public NoTransactionException() {
            super("Bad log file");
        }
    }

}
