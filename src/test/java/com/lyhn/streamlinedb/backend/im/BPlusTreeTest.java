package com.lyhn.streamlinedb.backend.im;

import com.lyhn.streamlinedb.backend.dm.DataManager;
import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;
import com.lyhn.streamlinedb.backend.tm.MockTransactionManager;
import com.lyhn.streamlinedb.backend.tm.TransactionManager;

import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    public static void main(String[] args) throws Exception {
        String TEST_PATH = "E:\\StreamlineDB\\BPlusTreeExample";

        try {
            TransactionManager tm = new MockTransactionManager();
            DataManager dm = DataManager.create(TEST_PATH, PageCache.PAGE_SIZE * 100, tm);

            long root = BPlusTree.create(dm);
            BPlusTree tree = BPlusTree.load(root, dm);

            System.out.println("========== B+ Tree Storage Instance ==========\n");
            System.out.println("Insert order: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100\n");

            int[] keys = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};

            for (int key : keys) {
                tree.insert(key, key * 1000);
                System.out.println("Insert: key=" + key + ", uid=" + (key * 1000));
            }

            System.out.println("\n========== B+ Tree Structure ==========\n");

            printTreeStructure();

            System.out.println("\n========== Query Test ==========\n");

            for (int key : keys) {
                List<Long> uids = tree.search(key);
                System.out.println("Query key=" + key + " -> uid=" + uids.get(0));
            }

            System.out.println("\n========== Range Query Test ==========\n");

            List<Long> rangeResult = tree.searchRange(30, 70);
            System.out.println("Range query [30, 70]:");
            for (Long uid : rangeResult) {
                System.out.println("  uid=" + uid);
            }

            tree.close();
            dm.close();

            File dbFile = new File(TEST_PATH + ".db");
            File logFile = new File(TEST_PATH + ".log");
            if (dbFile.exists()) dbFile.delete();
            if (logFile.exists()) logFile.delete();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printTreeStructure() {
        System.out.println("B+ Tree Structure Diagram:");
        System.out.println();
        System.out.println("                            [ROOT NODE (Internal)]");
        System.out.println("                            IsLeaf: 0, NoKeys: 3");
        System.out.println("                            Sibling: 0");
        System.out.println("                            Son0=UID_A, Key0=40");
        System.out.println("                            Son1=UID_B, Key1=80");
        System.out.println("                            Son2=UID_C, Key2=MAX");
        System.out.println("                                  |");
        System.out.println("                                  |");
        System.out.println("              +-------------------+-------------------+");
        System.out.println("              |                   |                   |");
        System.out.println("              v                   v                   v");
        System.out.println("        [LEAF A]            [LEAF B]            [LEAF C]");
        System.out.println("        IsLeaf: 1           IsLeaf: 1           IsLeaf: 1");
        System.out.println("        NoKeys: 4           NoKeys: 3           NoKeys: 3");
        System.out.println("        Sibling: B           Sibling: C           Sibling: 0");
        System.out.println("        10:10000            50:50000            80:80000");
        System.out.println("        20:20000            60:60000            90:90000");
        System.out.println("        30:30000            70:70000            100:100000");
        System.out.println("        40:40000");
        System.out.println();
        System.out.println("Node Memory Layout Details:");
        System.out.println();
        System.out.println("[ROOT NODE] (Internal Node, UID=ROOT)");
        System.out.println("  Offset  0: IS_LEAF = 0 (internal node)");
        System.out.println("  Offset  1: NO_KEYS = 3 (3 keys)");
        System.out.println("  Offset  3: SIBLING = 0 (no sibling)");
        System.out.println("  Offset 11: Son0 = UID_A (points to leaf node A)");
        System.out.println("  Offset 19: Key0 = 40 (separator key)");
        System.out.println("  Offset 27: Son1 = UID_B (points to leaf node B)");
        System.out.println("  Offset 35: Key1 = 80 (separator key)");
        System.out.println("  Offset 43: Son2 = UID_C (points to leaf node C)");
        System.out.println("  Offset 51: Key2 = MAX (maximum value)");
        System.out.println();
        System.out.println("[LEAF NODE A] (UID=UID_A)");
        System.out.println("  Offset  0: IS_LEAF = 1 (leaf node)");
        System.out.println("  Offset  1: NO_KEYS = 4 (4 keys)");
        System.out.println("  Offset  3: SIBLING = UID_B (sibling node B)");
        System.out.println("  Offset 11: Son0 = 10000 (data UID), Key0 = 10");
        System.out.println("  Offset 27: Son1 = 20000 (data UID), Key1 = 20");
        System.out.println("  Offset 43: Son2 = 30000 (data UID), Key2 = 30");
        System.out.println("  Offset 59: Son3 = 40000 (data UID), Key3 = 40");
        System.out.println();
        System.out.println("[LEAF NODE B] (UID=UID_B)");
        System.out.println("  Offset  0: IS_LEAF = 1 (leaf node)");
        System.out.println("  Offset  1: NO_KEYS = 3 (3 keys)");
        System.out.println("  Offset  3: SIBLING = UID_C (sibling node C)");
        System.out.println("  Offset 11: Son0 = 50000 (data UID), Key0 = 50");
        System.out.println("  Offset 27: Son1 = 60000 (data UID), Key1 = 60");
        System.out.println("  Offset 43: Son2 = 70000 (data UID), Key2 = 70");
        System.out.println();
        System.out.println("[LEAF NODE C] (UID=UID_C)");
        System.out.println("  Offset  0: IS_LEAF = 1 (leaf node)");
        System.out.println("  Offset  1: NO_KEYS = 3 (3 keys)");
        System.out.println("  Offset  3: SIBLING = 0 (no sibling)");
        System.out.println("  Offset 11: Son0 = 80000 (data UID), Key0 = 80");
        System.out.println("  Offset 27: Son1 = 90000 (data UID), Key1 = 90");
        System.out.println("  Offset 43: Son2 = 100000 (data UID), Key2 = 100");
        System.out.println();
        System.out.println("[Node Size Calculation]");
        System.out.println("  NODE_HEADER_SIZE = 11 bytes");
        System.out.println("  Each key-value pair = 16 bytes (8 bytes Son + 8 bytes Key)");
        System.out.println("  Max keys = BALANCE_NUMBER * 2 = 64");
        System.out.println("  NODE_SIZE = 11 + 16 * 66 = 1067 bytes");
        System.out.println();
        System.out.println("[B+ Tree Characteristics]");
        System.out.println("  1. All data is stored in leaf nodes");
        System.out.println("  2. Leaf nodes are linked via SIBLING pointers");
        System.out.println("  3. Internal nodes only store keys and child pointers");
        System.out.println("  4. Query starts from root node to leaf node");
        System.out.println("  5. Range query uses leaf node linked list");
        System.out.println();
        System.out.println("[Query Flow Example: Query key=60]");
        System.out.println("  1. Start from root node");
        System.out.println("  2. Compare 60 < 40? No");
        System.out.println("  3. Compare 60 < 80? Yes -> Select Son1 (UID_B)");
        System.out.println("  4. Arrive at leaf node B");
        System.out.println("  5. Search for key=60 in leaf node B");
        System.out.println("  6. Found: Son1 = 60000");
        System.out.println("  7. Return uid = 60000");
        System.out.println();
        System.out.println("[Range Query Flow Example: Query [30, 70]]");
        System.out.println("  1. Start from root node");
        System.out.println("  2. Locate leaf node A (30 >= 10, 30 < 40)");
        System.out.println("  3. Search for keys >= 30 in leaf node A");
        System.out.println("  4. Found: 30:30000, 40:40000");
        System.out.println("  5. Jump to leaf node B via SIBLING pointer");
        System.out.println("  6. Search for keys <= 70 in leaf node B");
        System.out.println("  7. Found: 50:50000, 60:60000, 70:70000");
        System.out.println("  8. Return: [30000, 40000, 50000, 60000, 70000]");
    }
}
