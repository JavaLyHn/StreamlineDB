package com.lyhn.streamlinedb.backend.vm;
import java.util.HashMap;
import java.util.Map;
public class DeadlockDFSExample {
    public static void main(String[] args) {
        System.out.println("========== Deadlock Detection DFS Algorithm Example ==========\n");

        Map<Long, Long> u2x = new HashMap<>();
        Map<Long, Long> waitU = new HashMap<>();

        long T1 = 1, T2 = 2, T3 = 3;
        long R1 = 101, R2 = 102, R3 = 103;

        u2x.put(R1, T1);
        waitU.put(T1, R2);

        u2x.put(R2, T2);
        waitU.put(T2, R1);

        u2x.put(R3, T3);
        waitU.put(T3, R2);

        System.out.println("Initial State:");
        System.out.println("  T1 holds R1, waits for R2");
        System.out.println("  T2 holds R2, waits for R1");
        System.out.println("  T3 holds R3, waits for R2");
        System.out.println();

        System.out.println("Starting DFS deadlock detection...\n");

        Map<Long, Integer> xidStamp = new HashMap<>();
        int stamp = 0;

        for (long xid : new long[]{T1, T2, T3}) {
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0) {
                System.out.println("  Transaction T" + xid + " already visited, skip");
                continue;
            }
            stamp++;
            System.out.println("\n=== Starting DFS from T" + xid + " (stamp=" + stamp + ") ===");

            if (dfs(xid, u2x, waitU, xidStamp, stamp)) {
                System.out.println("\n[Deadlock Detected!]");
                return;
            }
        }

        System.out.println("\n[No Deadlock Detected]");
    }

    private static boolean dfs(long xid, Map<Long, Long> u2x, Map<Long, Long> waitU,
                               Map<Long, Integer> xidStamp, int stamp) {
        Integer stp = xidStamp.get(xid);

        System.out.println("  DFS(T" + xid + ")");

        if (stp != null && stp == stamp) {
            System.out.println("    -> Found T" + xid + " with stamp = " + stamp + " (current stamp), cycle detected!");
            return true;
        }

        if (stp != null && stp < stamp) {
            System.out.println("    -> T" + xid + " has stamp = " + stp + " < " + stamp + ", already visited, no cycle");
            return false;
        }

        xidStamp.put(xid, stamp);
        System.out.println("    -> Mark T" + xid + " with stamp = " + stamp);

        Long uid = waitU.get(xid);
        if (uid == null) {
            System.out.println("    -> T" + xid + " does not wait for any resource, backtrack");
            return false;
        }

        System.out.println("    -> T" + xid + " waits for resource R" + uid);

        Long x = u2x.get(uid);
        if (x == null) {
            System.out.println("    -> Resource R" + uid + " is not held by any transaction, backtrack");
            return false;
        }

        System.out.println("    -> Resource R" + uid + " is held by T" + x);
        System.out.println("    -> Continue DFS(T" + x + ")");

        return dfs(x, u2x, waitU, xidStamp, stamp);
    }
}
