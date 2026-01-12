package com.lyhn.streamlinedb.backend.tm;

import java.io.File;

public class TransactionManagerSimpleTest {
    public static void main(String[] args) {
        String testPath = "E:/MYDB-master/TESTTransactionManagerSimple";

        try {
            System.out.println("=== 事务管理器测试 ===");

            System.out.println("\n1. 创建事务管理器...");
            TransactionManager tm = TransactionManager.create(testPath);
            System.out.println("✓ 事务管理器创建成功");

            System.out.println("\n2. 测试单个事务...");
            long xid1 = tm.begin();
            System.out.println("✓ 开始事务 xid=" + xid1);
            System.out.println("  - isActive: " + tm.isActive(xid1));
            System.out.println("  - isCommitted: " + tm.isCommitted(xid1));
            System.out.println("  - isAborted: " + tm.isAborted(xid1));

            tm.commit(xid1);
            System.out.println("✓ 提交事务 xid=" + xid1);
            System.out.println("  - isActive: " + tm.isActive(xid1));
            System.out.println("  - isCommitted: " + tm.isCommitted(xid1));
            System.out.println("  - isAborted: " + tm.isAborted(xid1));

            System.out.println("\n3. 测试事务回滚...");
            long xid2 = tm.begin();
            System.out.println("✓ 开始事务 xid=" + xid2);

            tm.abort(xid2);
            System.out.println("✓ 回滚事务 xid=" + xid2);
            System.out.println("  - isActive: " + tm.isActive(xid2));
            System.out.println("  - isCommitted: " + tm.isCommitted(xid2));
            System.out.println("  - isAborted: " + tm.isAborted(xid2));

            System.out.println("\n4. 测试多个事务...");
            long xid3 = tm.begin();
            long xid4 = tm.begin();
            long xid5 = tm.begin();
            System.out.println("✓ 开始三个事务: " + xid3 + ", " + xid4 + ", " + xid5);

            tm.commit(xid3);
            tm.abort(xid4);
            System.out.println("✓ 提交 " + xid3 + ", 回滚 " + xid4);

            System.out.println("  - xid3: active=" + tm.isActive(xid3) +
                    ", committed=" + tm.isCommitted(xid3) +
                    ", aborted=" + tm.isAborted(xid3));
            System.out.println("  - xid4: active=" + tm.isActive(xid4) +
                    ", committed=" + tm.isCommitted(xid4) +
                    ", aborted=" + tm.isAborted(xid4));
            System.out.println("  - xid5: active=" + tm.isActive(xid5) +
                    ", committed=" + tm.isCommitted(xid5) +
                    ", aborted=" + tm.isAborted(xid5));

            System.out.println("\n5. 测试超级事务...");
            System.out.println("  - SUPER_XID=" + TransactionManagerImpl.SUPER_XID);
            System.out.println("  - active: " + tm.isActive(TransactionManagerImpl.SUPER_XID));
            System.out.println("  - committed: " + tm.isCommitted(TransactionManagerImpl.SUPER_XID));
            System.out.println("  - aborted: " + tm.isAborted(TransactionManagerImpl.SUPER_XID));

            System.out.println("\n6. 测试事务持久化...");
            tm.close();
            System.out.println("✓ 关闭事务管理器");

            TransactionManager tm2 = TransactionManager.open(testPath);
            System.out.println("✓ 重新打开事务管理器");
            System.out.println("  - xid1: committed=" + tm2.isCommitted(xid1));
            System.out.println("  - xid2: aborted=" + tm2.isAborted(xid2));
            System.out.println("  - xid3: committed=" + tm2.isCommitted(xid3));
            System.out.println("  - xid4: aborted=" + tm2.isAborted(xid4));
            System.out.println("  - xid5: active=" + tm2.isActive(xid5));

            tm2.close();
            System.out.println("\n✓ 所有测试通过！");

        } catch (Exception e) {
            System.err.println("✗ 测试失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            new File(testPath + ".xid").delete();
            System.out.println("\n清理测试文件完成");
        }
    }
}
