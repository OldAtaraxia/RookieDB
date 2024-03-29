package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.categories.Proj4Part2Tests;
import edu.berkeley.cs186.database.categories.Proj4Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.concurrency.IsolationLevel;
import edu.berkeley.cs186.database.concurrency.LoggingLockManager;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({Proj4Tests.class, Proj4Part2Tests.class})
public class TestDatabase2PL {
    private static final String TestDir = "testDatabase2PL";
    private static boolean passedPreCheck = false;
    private Database db;
    private LoggingLockManager lockManager;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 7 second max per method tested.
    public static long timeout = (long) (7000 * TimeoutScaling.factor);

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis(timeout));

    private void reloadDatabase() {
        if (this.db != null) {
            while (TransactionContext.getTransaction() != null) {
                TransactionContext.unsetTransaction();
            }
            this.db.close();
        }
        if (this.lockManager != null && this.lockManager.isLogging()) {
            List<String> oldLog = this.lockManager.log;
            this.lockManager = new LoggingLockManager();
            this.lockManager.log = oldLog;
            this.lockManager.startLog();
        } else {
            this.lockManager = new LoggingLockManager();
        }
        this.db = new Database(this.filename, 128, this.lockManager);
        this.db.setWorkMem(32); // B=32
        // force initialization to finish before continuing
        this.db.waitAllTransactions();
    }

    @ClassRule
    public static  TemporaryFolder checkFolder = new TemporaryFolder();

    @BeforeClass
    public static void beforeAll() {
        passedPreCheck = TestDatabaseDeadlockPrecheck.performCheck(checkFolder);
    }

    @Before
    public void beforeEach() throws Exception {
        assertTrue("You will need to pass the test in testDatabaseDeadLockPrecheck before running these tests", passedPreCheck);
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.reloadDatabase();
        try(Transaction t = this.beginTransaction()) {
            t.dropAllTables();
        } finally {
            this.db.waitAllTransactions();
        }
    }

    @After
    public void afterEach() {
        if (!passedPreCheck) {
            return;
        }

        this.lockManager.endLog();
        while (TransactionContext.getTransaction() != null) {
            TransactionContext.unsetTransaction();
        }
        this.db.close();
    }

    private Transaction beginTransaction() {
        // Database.Transaction ordinarily calls setTransaction/unsetTransaction around calls,
        // but we test directly with TransactionContext calls here, so we need to call setTransaction
        // manually
        Transaction t = db.beginTransaction();
        TransactionContext.setTransaction(t.getTransactionContext());
        return t;
    }

    private static <T extends Comparable<? super T>> void assertSameItems(List<T> expected,
                                                                          List<T> actual) {
        Collections.sort(expected);
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

    private static <T> void assertSubsequence(List<T> expected, List<T> actual) {
        if (expected.size() == 0) {
            return;
        }
        Iterator<T> ei = expected.iterator();
        Iterator<T> ai = actual.iterator();
        while (ei.hasNext()) {
            T next = ei.next();
            boolean found = false;
            while (ai.hasNext()) {
                if (ai.next().equals(next)) {
                    found = true;
                    break;
                }
            }
            assertTrue(expected + " not subsequence of " + actual, found);
        }
    }

    private static List<String> prepare(Long transNum, String ... expected) {
        return Arrays.stream(expected).map((String log) -> String.format(log,
                transNum)).collect(Collectors.toList());
    }

    private static List<String> removeMetadataLogs(List<String> log) {
        log = new ArrayList<>(log);
        // remove all _metadata lock log entries
        log.removeIf((String x) -> x.contains("_metadata"));
        // replace [acquire IS(database), promote IX(database), ...] with [acquire IX(database), ...]
        // (as if the _metadata locks never happened)
        if (log.size() >= 2 && log.get(0).endsWith("database IS") && log.get(1).endsWith("database IX")) {
            log.set(0, log.get(0).replace("IS", "IX"));
            log.remove(1);
        }
        return log;
    }

    private List<RecordId> createTable(String tableName, int pages) {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();
        List<RecordId> rids = new ArrayList<>();
        try(Transaction t1 = beginTransaction()) {
            t1.createTable(s, tableName);
            int numRecords = pages * t1.getTransactionContext().getTable(tableName).getNumRecordsPerPage();
            for (int i = 0; i < numRecords; ++i) {
                rids.add(t1.getTransactionContext().addRecord(tableName, input));
            }
        } finally {
            this.db.waitAllTransactions();
        }

        return rids;
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordRead() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            // Read first record
            t1.getTransactionContext().getRecord(tableName, rids.get(0));
            // Read record on 3rd data page
            t1.getTransactionContext().getRecord(tableName, rids.get(3 * rids.size() / 4 - 1));
            // Read last record
            t1.getTransactionContext().getRecord(tableName, rids.get(rids.size() - 1));
            assertEquals(prepare(t1.getTransNum(),
                    "acquire %s database IS",
                    "acquire %s database/testtable1 IS",
                    "acquire %s database/testtable1/30000000001 S",
                    "acquire %s database/testtable1/30000000003 S",
                    "acquire %s database/testtable1/30000000004 S"
            ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleTransactionCleanup() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        Transaction t1 = beginTransaction();
        try {
            // Read record on first page
            t1.getTransactionContext().getRecord(tableName, rids.get(0));
            // Read record on third page
            t1.getTransactionContext().getRecord(tableName, rids.get(3 * rids.size() / 4 - 1));
            // Read record on last page
            t1.getTransactionContext().getRecord(tableName, rids.get(rids.size() - 1));

            // Should have IS(db), IS(db/testtable1), and 3 S locks on pages
            assertTrue("did not acquire all required locks",
                    lockManager.getLocks(t1.getTransactionContext()).size() >= 5);

            lockManager.startLog();
        } finally {
            t1.commit();
            this.db.waitAllTransactions();
        }

        // After committing the transaction should release all locks
        assertTrue("did not free all required locks",
                lockManager.getLocks(t1.getTransactionContext()).isEmpty());
        assertSubsequence(prepare(t1.getTransNum(),
                "release %s database/testtable1/30000000003",
                "release %s database"
        ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordWrite() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        Record input = TestUtils.createRecordWithAllTypes();

        try(Transaction t0 = beginTransaction()) {
            t0.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() - 1));
        } finally {
            this.db.waitAllTransactions();
        }

        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            t1.insert(tableName, input);
            // Insert a new record onto the last page of the table
            assertEquals(prepare(t1.getTransNum(),
                    "acquire %s database IX",
                    "acquire %s database/testtable1 IX",
                    "acquire %s database/testtable1/30000000004 X"
            ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordReadWrite() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        Record input = TestUtils.createRecordWithAllTypes();

        try(Transaction t0 = beginTransaction()) {
            t0.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() - 1));
        } finally {
            this.db.waitAllTransactions();
        }

        lockManager.startLog();
        try(Transaction t1 = beginTransaction()) {
            // Read the first record
            t1.getTransactionContext().getRecord(tableName, rids.get(0));
            // Insert a new record onto the last page
            t1.insert(tableName, input);

            assertEquals(prepare(t1.getTransNum(),
                    "acquire %s database IS",
                    "acquire %s database/testtable1 IS",
                    "acquire %s database/testtable1/30000000001 S",
                    "promote %s database IX",
                    "promote %s database/testtable1 IX",
                    "acquire %s database/testtable1/30000000004 X"
            ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordUpdate() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        Record input = TestUtils.createRecordWithAllTypes();

        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            // Update the last record in the table
            t1.getTransactionContext().updateRecord(tableName, rids.get(rids.size() - 1), input);

            assertEquals(prepare(t1.getTransNum(),
                    "acquire %s database IX",
                    "acquire %s database/testtable1 IX",
                    "acquire %s database/testtable1/30000000004 X"
            ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordDelete() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            // Delete the last record in the table
            t1.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() - 1));
            assertEquals(prepare(t1.getTransNum(),
                    "acquire %s database IX",
                    "acquire %s database/testtable1 IX",
                    "acquire %s database/testtable1/30000000004 X"
            ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void test2PLDeadLock() throws InterruptedException {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        lockManager.startLog();
        System.out.println(rids.size());
        // 死锁情况:
        // Transaction 1: X(A)           X(B)
        // Transaction 2:      X(B)  X(A)
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction t1 = beginTransaction();
                // X(A)
                t1.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() - 3));
                System.out.println(lockManager.getLocks(t1.getTransactionContext()));
                // 休眠
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //X(B)
                t1.getTransactionContext().deleteRecord(tableName, rids.get(2));
                System.out.println(lockManager.getLocks(t1.getTransactionContext()));

                System.out.println("t1 is about to be closed");
                t1.close();
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction t2 = beginTransaction();
                // 休眠
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // X(B)
                t2.getTransactionContext().deleteRecord(tableName, rids.get(1));
                System.out.println(lockManager.getLocks(t2.getTransactionContext()));
                //X(A)
                t2.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() - 2));
                System.out.println(lockManager.getLocks(t2.getTransactionContext()));

                System.out.println("t2 is about to be closed");
                t2.close();
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    @Test
    @Category(PublicTests.class)
    public void testReadCommited() throws InterruptedException {
        Database.setDBIsolationLevel(IsolationLevel.READ_COMMITTED);
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        lockManager.startLog();
        System.out.println(rids.size());
        final Record[] records = new Record[2];
        // 这种隔离等级下可以读到commited事务的修改
        // 不可重复读的案例
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction t1 = beginTransaction();
                System.out.println("t1 has read " + t1.getTransactionContext().getRecord(tableName, rids.get(1)));
                // 先休眠3s等t2完成
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                records[0] = t1.getTransactionContext().getRecord(tableName, rids.get(1));
                System.out.println("t1 has read " + records[0]);
            }
        });

        records[1] = new Record(true, 2, "b", 3.2f);
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction t2 = beginTransaction();
                t2.getTransactionContext().updateRecord(tableName, rids.get(1), records[1]);
                System.out.println("t2 updates record " + records[1]);
                System.out.println("t2 is about to be closed");
                t2.close();
            }
        });
        //
        t1.start();
        // 休眠1s后开启事务T2
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t2.start();
        t1.join();
        t2.join();
        assertEquals(records[0], records[1]);
    }

    @Test
    @Category(PublicTests.class)
    public void testReadUnCommitted () throws InterruptedException {
        Database.setDBIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        lockManager.startLog();
        System.out.println(rids.size());
        final Record[] records = new Record[2];
        // 这种隔离等级下可以读到commited事务的修改
        // 不可重复读的案例
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction t1 = beginTransaction();
                System.out.println("t1 has read " + t1.getTransactionContext().getRecord(tableName, rids.get(1)));
                // 先休眠3s等t2启动
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 这时T2应该还没有commit
                records[0] = t1.getTransactionContext().getRecord(tableName, rids.get(1));
                System.out.println("t1 has read " + records[0]);
            }
        });

        records[1] = new Record(true, 2, "b", 3.2f);
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Transaction t2 = beginTransaction();
                t2.getTransactionContext().updateRecord(tableName, rids.get(1), records[1]);
                System.out.println("t2 updates record " + records[1]);
                System.out.println("t2 is about to be closed");
                // 休眠5s让T1读到T2的Uncommitted message
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                t2.close();
            }
        });
        //
        t1.start();
        // 休眠1s后开启事务T2
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t2.start();
        t1.join();
        t2.join();
        assertEquals(records[0], records[1]);
    }
}
