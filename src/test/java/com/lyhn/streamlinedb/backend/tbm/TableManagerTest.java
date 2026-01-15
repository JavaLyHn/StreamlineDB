package com.lyhn.streamlinedb.backend.tbm;

import com.lyhn.streamlinedb.backend.dm.DataManager;
import com.lyhn.streamlinedb.backend.parser.statement.*;
import com.lyhn.streamlinedb.backend.tm.TransactionManager;
import com.lyhn.streamlinedb.backend.vm.VersionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TableManagerTest {
    private static final int PAGE_SIZE = 8192;
    private String TEST_PATH;

    private DataManager dm;
    private TransactionManager tm;
    private VersionManager vm;
    private TableManager tbm;

    @Before
    public void setUp() throws Exception {
        TEST_PATH = System.getProperty("java.io.tmpdir") + File.separator + "TableManagerImplTest_" + System.currentTimeMillis();
        File testDir = new File(TEST_PATH);
        if (testDir.exists()) {
            deleteDirectory(testDir);
        }
        testDir.mkdirs();

        tm = TransactionManager.create(TEST_PATH);
        dm = DataManager.create(TEST_PATH, PAGE_SIZE * 100, tm);
        vm = VersionManager.newVersionManager(tm, dm);
        tbm = TableManager.create(TEST_PATH, vm, dm);
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (tbm != null) {
                tbm = null;
            }
            if (vm != null) {
                vm = null;
            }
            if (dm != null) {
                dm.close();
            }
            if (tm != null) {
                tm.close();
            }
        } catch (Exception e) {
        }

        File testDir = new File(TEST_PATH);
        if (testDir.exists()) {
            deleteDirectory(testDir);
        }
    }

    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    @Test
    public void testCreateTable() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "test_table";
        create.fieldName = new String[]{"id", "name", "age"};
        create.fieldType = new String[]{"int32", "string", "int32"};
        create.index = new String[]{"id", "name", null};

        byte[] result = tbm.create(xid, create);
        assert new String(result).equals("create test_table");

        tbm.commit(xid);
    }

    @Test
    public void testCreateDuplicateTable() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "test_table";
        create.fieldName = new String[]{"id", "name"};
        create.fieldType = new String[]{"int32", "string"};
        create.index = new String[]{"id", null};

        tbm.create(xid, create);

        try {
            tbm.create(xid, create);
            assert false;
        } catch (Exception e) {
            assert true;
        }

        tbm.commit(xid);
    }

    @Test
    public void testInsertAndRead() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "students";
        create.fieldName = new String[]{"id", "name", "age"};
        create.fieldType = new String[]{"int32", "string", "int32"};
        create.index = new String[]{"id", null, null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "students";
        insert.values = new String[]{"1", "Alice", "20"};
        tbm.insert(xid, insert);

        insert.values = new String[]{"2", "Bob", "21"};
        tbm.insert(xid, insert);

        insert.values = new String[]{"3", "Charlie", "22"};
        tbm.insert(xid, insert);

        Select select = new Select();
        select.tableName = "students";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        String resultStr = new String(readResult);
        assert resultStr.contains("Alice");
        assert resultStr.contains("Bob");
        assert resultStr.contains("Charlie");

        tbm.commit(xid);
    }

    @Test
    public void testUpdate() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "employees";
        create.fieldName = new String[]{"id", "name", "salary"};
        create.fieldType = new String[]{"int32", "string", "int32"};
        create.index = new String[]{"id", null, null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "employees";
        insert.values = new String[]{"1", "John", "5000"};
        tbm.insert(xid, insert);

        insert.values = new String[]{"2", "Jane", "6000"};
        tbm.insert(xid, insert);

        Update update = new Update();
        update.tableName = "employees";
        update.fieldName = "salary";
        update.value = "5500";
        update.where = null;

        byte[] updateResult = tbm.update(xid, update);
        String resultStr = new String(updateResult);
        assert resultStr.equals("update 2");

        Select select = new Select();
        select.tableName = "employees";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        resultStr = new String(readResult);
        assert resultStr.contains("5500");

        tbm.commit(xid);
    }

    @Test
    public void testDelete() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "products";
        create.fieldName = new String[]{"id", "name", "price"};
        create.fieldType = new String[]{"int32", "string", "int32"};
        create.index = new String[]{"id", null, null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "products";
        insert.values = new String[]{"1", "Product1", "100"};
        tbm.insert(xid, insert);

        insert.values = new String[]{"2", "Product2", "200"};
        tbm.insert(xid, insert);

        insert.values = new String[]{"3", "Product3", "300"};
        tbm.insert(xid, insert);

        Delete delete = new Delete();
        delete.tableName = "products";
        delete.where = null;

        byte[] deleteResult = tbm.delete(xid, delete);
        String resultStr = new String(deleteResult);
        assert resultStr.equals("delete 3");

        Select select = new Select();
        select.tableName = "products";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        resultStr = new String(readResult);
        assert !resultStr.contains("Product1");
        assert !resultStr.contains("Product2");
        assert !resultStr.contains("Product3");

        tbm.commit(xid);
    }

    @Test
    public void testShowTables() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "table1";
        create.fieldName = new String[]{"id"};
        create.fieldType = new String[]{"int32"};
        create.index = new String[]{"id"};

        tbm.create(xid, create);

        create.tableName = "table2";
        create.fieldName = new String[]{"id", "name"};
        create.fieldType = new String[]{"int32", "string"};
        create.index = new String[]{"id", null};

        tbm.create(xid, create);

        byte[] showResult = tbm.show(xid);
        String resultStr = new String(showResult);
        assert resultStr.contains("table1");
        assert resultStr.contains("table2");

        tbm.commit(xid);
    }

    @Test
    public void testTransactionCommit() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "commit_test";
        create.fieldName = new String[]{"id", "value"};
        create.fieldType = new String[]{"int32", "int32"};
        create.index = new String[]{"id", null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "commit_test";
        insert.values = new String[]{"1", "100"};
        tbm.insert(xid, insert);

        byte[] commitResult = tbm.commit(xid);
        assert new String(commitResult).equals("commit");

        begin = new Begin();
        begin.isRepeatableRead = false;
        beginRes = tbm.begin(begin);
        xid = beginRes.xid;

        Select select = new Select();
        select.tableName = "commit_test";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        String resultStr = new String(readResult);
        assert resultStr.contains("100");

        tbm.commit(xid);
    }

    @Test
    public void testTransactionAbort() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "abort_test";
        create.fieldName = new String[]{"id", "value"};
        create.fieldType = new String[]{"int32", "int32"};
        create.index = new String[]{"id", null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "abort_test";
        insert.values = new String[]{"1", "100"};
        tbm.insert(xid, insert);

        byte[] abortResult = tbm.abort(xid);
        assert new String(abortResult).equals("abort");

        begin = new Begin();
        begin.isRepeatableRead = false;
        beginRes = tbm.begin(begin);
        xid = beginRes.xid;

        Select select = new Select();
        select.tableName = "abort_test";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        String resultStr = new String(readResult);
        assert !resultStr.contains("100");

        tbm.commit(xid);
    }

    @Test
    public void testMultipleTransactions() throws Exception {
        Create create = new Create();
        create.tableName = "multi_test";
        create.fieldName = new String[]{"id", "value"};
        create.fieldType = new String[]{"int32", "int32"};
        create.index = new String[]{"id", null};

        Begin begin1 = new Begin();
        begin1.isRepeatableRead = false;
        BeginRes beginRes1 = tbm.begin(begin1);
        long xid1 = beginRes1.xid;

        tbm.create(xid1, create);

        Insert insert = new Insert();
        insert.tableName = "multi_test";
        insert.values = new String[]{"1", "100"};
        tbm.insert(xid1, insert);

        tbm.commit(xid1);

        Begin begin2 = new Begin();
        begin2.isRepeatableRead = false;
        BeginRes beginRes2 = tbm.begin(begin2);
        long xid2 = beginRes2.xid;

        insert.values = new String[]{"2", "200"};
        tbm.insert(xid2, insert);

        tbm.commit(xid2);

        Begin begin3 = new Begin();
        begin3.isRepeatableRead = false;
        BeginRes beginRes3 = tbm.begin(begin3);
        long xid3 = beginRes3.xid;

        Select select = new Select();
        select.tableName = "multi_test";
        select.where = null;

        byte[] readResult = tbm.read(xid3, select);
        String resultStr = new String(readResult);
        assert resultStr.contains("100");
        assert resultStr.contains("200");

        tbm.commit(xid3);
    }

    @Test
    public void testStringIndex() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "string_index_test";
        create.fieldName = new String[]{"id", "name", "age"};
        create.fieldType = new String[]{"int32", "string", "int32"};
        create.index = new String[]{"id", "name", null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "string_index_test";
        insert.values = new String[]{"1", "Alice", "20"};
        tbm.insert(xid, insert);

        insert.values = new String[]{"2", "Bob", "21"};
        tbm.insert(xid, insert);

        insert.values = new String[]{"3", "Charlie", "22"};
        tbm.insert(xid, insert);

        Select select = new Select();
        select.tableName = "string_index_test";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        String resultStr = new String(readResult);
        assert resultStr.contains("Alice");
        assert resultStr.contains("Bob");
        assert resultStr.contains("Charlie");

        tbm.commit(xid);
    }

    @Test
    public void testInt64Type() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "int64_test";
        create.fieldName = new String[]{"id", "timestamp", "value"};
        create.fieldType = new String[]{"int32", "int64", "int32"};
        create.index = new String[]{"id", "timestamp", null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "int64_test";
        insert.values = new String[]{"1", "1234567890123", "100"};
        tbm.insert(xid, insert);

        insert.values = new String[]{"2", "1234567890124", "200"};
        tbm.insert(xid, insert);

        Select select = new Select();
        select.tableName = "int64_test";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        String resultStr = new String(readResult);
        assert resultStr.contains("1234567890123");
        assert resultStr.contains("1234567890124");

        tbm.commit(xid);
    }

    @Test
    public void testLargeDataset() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "large_test";
        create.fieldName = new String[]{"id", "value"};
        create.fieldType = new String[]{"int32", "int32"};
        create.index = new String[]{"id", null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "large_test";

        for (int i = 1; i <= 1000; i++) {
            insert.values = new String[]{String.valueOf(i), String.valueOf(i * 10)};
            tbm.insert(xid, insert);
        }

        Select select = new Select();
        select.tableName = "large_test";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        String resultStr = new String(readResult);
        assert resultStr.contains("10");
        assert resultStr.contains("10000");

        tbm.commit(xid);
    }

    @Test
    public void testTablePersistence() throws Exception {
        Begin begin = new Begin();
        begin.isRepeatableRead = false;
        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;

        Create create = new Create();
        create.tableName = "persistence_test";
        create.fieldName = new String[]{"id", "name"};
        create.fieldType = new String[]{"int32", "string"};
        create.index = new String[]{"id", null};

        tbm.create(xid, create);

        Insert insert = new Insert();
        insert.tableName = "persistence_test";
        insert.values = new String[]{"1", "TestName"};
        tbm.insert(xid, insert);

        tbm.commit(xid);

        dm.close();

        tm = TransactionManager.open(TEST_PATH);
        dm = DataManager.open(TEST_PATH, PAGE_SIZE * 100, tm);
        vm = VersionManager.newVersionManager(tm, dm);
        tbm = TableManager.open(TEST_PATH, vm, dm);

        begin = new Begin();
        begin.isRepeatableRead = false;
        beginRes = tbm.begin(begin);
        xid = beginRes.xid;

        Select select = new Select();
        select.tableName = "persistence_test";
        select.where = null;

        byte[] readResult = tbm.read(xid, select);
        String resultStr = new String(readResult);
        assert resultStr.contains("TestName");

        tbm.commit(xid);
    }

    @Test
    public void testConcurrentInserts() throws Exception {
        Create create = new Create();
        create.tableName = "concurrent_test";
        create.fieldName = new String[]{"id", "value"};
        create.fieldType = new String[]{"int32", "int32"};
        create.index = new String[]{"id", null};

        Begin begin1 = new Begin();
        begin1.isRepeatableRead = false;
        BeginRes beginRes1 = tbm.begin(begin1);
        long xid1 = beginRes1.xid;

        tbm.create(xid1, create);

        Insert insert = new Insert();
        insert.tableName = "concurrent_test";
        for (int i = 1; i <= 50; i++) {
            insert.values = new String[]{String.valueOf(i), String.valueOf(i * 10)};
            tbm.insert(xid1, insert);
        }

        tbm.commit(xid1);

        Begin begin2 = new Begin();
        begin2.isRepeatableRead = false;
        BeginRes beginRes2 = tbm.begin(begin2);
        long xid2 = beginRes2.xid;

        for (int i = 51; i <= 100; i++) {
            insert.values = new String[]{String.valueOf(i), String.valueOf(i * 10)};
            tbm.insert(xid2, insert);
        }

        tbm.commit(xid2);

        Begin begin3 = new Begin();
        begin3.isRepeatableRead = false;
        BeginRes beginRes3 = tbm.begin(begin3);
        long xid3 = beginRes3.xid;

        Select select = new Select();
        select.tableName = "concurrent_test";
        select.where = null;

        byte[] readResult = tbm.read(xid3, select);
        String resultStr = new String(readResult);
        assert resultStr.contains("10");
        assert resultStr.contains("1000");

        tbm.commit(xid3);
    }
}
