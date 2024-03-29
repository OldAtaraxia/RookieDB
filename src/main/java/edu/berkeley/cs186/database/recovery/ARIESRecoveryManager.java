package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        // 写commit日志
        long lsn = this.logManager.appendToLog(new CommitTransactionLogRecord(transNum,
                this.transactionTable.get(transNum).lastLSN));
        // 将commit日志刷入disk
        this.logManager.flushToLSN(lsn);
        // 修改事务状态
        this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        // 更新lastLSN
        this.transactionTable.get(transNum).lastLSN = lsn;
        return lsn;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // 写入abort日志
        long lsn = this.logManager.appendToLog(new AbortTransactionLogRecord(transNum,
                this.transactionTable.get(transNum).lastLSN));
        // 更改事务状态
        this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);
        // 更新lastLSN
        this.transactionTable.get(transNum).lastLSN = lsn;
        return lsn;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        // 首先判断事务是否是因为ABORT而中止
        if (this.transactionTable.get(transNum).transaction.getStatus().equals(Transaction.Status.ABORTING)) {
            // 写入END日志之前需要回滚所有之前的操作
            // 循环遍历prevLSN直到找到事务的第一条操作
            LogRecord record = this.logManager.fetchLogRecord(this.transactionTable.get(transNum).lastLSN);
            while (record.getPrevLSN().isPresent()) {
                record = this.logManager.fetchLogRecord(record.getPrevLSN().get());
            }
            rollbackToLSN(transNum, record.LSN);
        }
        // 写入日志
        this.logManager.appendToLog(new EndTransactionLogRecord(transNum,
                this.transactionTable.get(transNum).lastLSN));
        // 更新事务状态
        this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
        // 从ATT中删除对应事务
        this.transactionTable.remove(transNum);
        return -1L;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Emit the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above
        while (currentLSN > LSN) {
            LogRecord logRecord = this.logManager.fetchLogRecord(currentLSN);
            if (logRecord.isUndoable()) {
                // 增加一条CLR记录
                LogRecord undoRecord = logRecord.undo(lastRecordLSN);
                this.logManager.appendToLog(undoRecord);
                // 调用record.redo来执行undo操作
                undoRecord.redo(this, diskSpaceManager, bufferManager);
                // 更新lastRecordLSN
                lastRecordLSN = undoRecord.LSN;
            }
            currentLSN = logRecord.getPrevLSN().isPresent() ? logRecord.getPrevLSN().get() : -1;
        }
        // 更新ATT中的lastLSN
        this.transactionTable.get(transNum).lastLSN = lastRecordLSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert transactionEntry != null;

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        long LSN = logManager.appendToLog(record);
        // update LSN
        transactionEntry.lastLSN = LSN;
        // 若修改的page不在DTT中就将其加入
        if (!dirtyPageTable.containsKey(pageNum)) dirtyPageTable.put(pageNum, LSN);
        // flush log
        logManager.fetchLogRecord(LSN);
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        // checkpoint中需要写入的DPT和ATT
        Map<Long, Long> chkptDPT = new HashMap<>(); // pageNum -> recLSN
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>(); // tranNum -> <Status, lastLSN>

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for (long pageNum : dirtyPageTable.keySet()) {
            // 已经无法容纳新的记录了
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, 0)) {
                // 将当前临时表写入EndCheckPoint日志
                LogRecord endCheckpointLogRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endCheckpointLogRecord);
                // 清空临时DPT
                chkptDPT.clear();
            }
            // 依然可以容纳新的记录
            chkptDPT.put(pageNum, dirtyPageTable.get(pageNum));
        }

        for (long tranNum : transactionTable.keySet()) {
            // 已经无法容纳新的记录了
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {
                // 将当前临时表写入EndCheckPoint日志
                LogRecord endCheckpointLogRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endCheckpointLogRecord);
                // 清空临时表
                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            // 依然可以容纳新的记录
            chkptTxnTable.put(tranNum, new Pair(transactionTable.get(tranNum).transaction.getStatus(), transactionTable.get(tranNum).lastLSN));
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT(); // 需要把已经不脏的Page从DPT清理出去
        this.restartUndo();
        this.checkpoint(); // recover结束之后写一个checkpoint
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related, update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records are processed, cleanup and end transactions that are in
     * the COMMITING state, and move all transactions in the RUNNING state to
     * RECOVERY_ABORTING/emit an abort record.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        // 从上个chechpoint开始遍历日志
        LogRecord logRecord;
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(LSN);
        while (logRecordIterator.hasNext()) {
            logRecord = logRecordIterator.next();
            // log record for tansaction operations: 需要更新ATT
            checkTransactionOperation(logRecord);
            // log records for Page Operations: 需要更新DPT
            checkPageOperation(logRecord);
            // log record for Transaction Status Changes: 需要更新ATT中的事务状态
            checkTransactionStatusChange(logRecord, endedTransactions);
            // log record for end checkpoint
            checkEndCheckpoint(logRecord, endedTransactions);
        }
        endTrascations();
    }

    /**
     * 在analysis的最后阶段把正在committing的事务我们直接向日志中写入END日志并把其从事务表中删除.
     * 对于在崩溃时还在运行的事务都应该视为崩溃并向日志中写入abort.
     */
    private void endTrascations() {
        // 遍历整个ATT
        for (long tranNum : transactionTable.keySet()) {
            Transaction transaction = transactionTable.get(tranNum).transaction;
            if (transaction.getStatus().equals(Transaction.Status.COMMITTING)) {
                // 对于commiting的事务将其标记为完成
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(tranNum, transactionTable.get(tranNum).lastLSN));
                transactionTable.remove(tranNum);
            } else if (transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                // 对于running的事务将其视为abort
                transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                transactionTable.get(tranNum).lastLSN = logManager.appendToLog(new AbortTransactionLogRecord(tranNum, transactionTable.get(tranNum).lastLSN));
            }
        }
    }

    /**
     * util function to deal with log record for tansaction operations
     */
    private void checkTransactionOperation(LogRecord logRecord) {
        if (logRecord.getTransNum().isPresent()) {
            Long transNum = logRecord.getTransNum().get();
            // 加入ATT
            if (!transactionTable.containsKey(transNum)) {
                // 调用startTransaction来添加到ATT
                startTransaction(newTransaction.apply(transNum));
            }
            //更新事务的lastLSN
            transactionTable.get(transNum).lastLSN = logRecord.getLSN();
        }
    }

    /**
     * util function to deal with log records for Page Operations
     */
    private void checkPageOperation(LogRecord logRecord) {
        long LSN = logRecord.getLSN();
        if (logRecord.getPageNum().isPresent()) {
            Long pageNum = logRecord.getPageNum().get();
            if (logRecord.getType().equals(LogType.UPDATE_PAGE) || logRecord.getType().equals(LogType.UNDO_UPDATE_PAGE)) {
                // 对Page进行了更新, 若DPT中不存在对应Page应该加入到DPT中
                dirtyPageTable.putIfAbsent(pageNum, LSN);
            }
            if (logRecord.getType().equals(LogType.FREE_PAGE) ||logRecord.getType().equals(LogType.UNDO_ALLOC_PAGE)) {
                // 相当于刷新了(删除了)Page, 也要从DPT中删掉
                dirtyPageTable.remove(pageNum);
            }
            // don't neew to do anything for AllocPage / UndoFreePage
        }
    }

    /**
    * util function to deal with log record for Transaction Status Changes
    * */
    private void checkTransactionStatusChange (LogRecord logRecord, Set<Long> endSet) {
        if (!logRecord.getTransNum().isPresent()) return;
        Transaction transaction = transactionTable.get(logRecord.getTransNum().get()).transaction;
        switch (logRecord.getType()) {
            case COMMIT_TRANSACTION:
                transaction.setStatus(Transaction.Status.COMMITTING);
                break;
            case ABORT_TRANSACTION:
                transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                break;
            case END_TRANSACTION:
                // 关闭事务 -> 设置事务状态 -> 从ATT中删除 -> 加入endSet用于之后的checkpoint过程
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(logRecord.getTransNum().get());
                endSet.add(logRecord.getTransNum().get());
                break;
            default: break;
        }
    }

    /**
     * util function to deal with log record for checkpoint
     */
    private void checkEndCheckpoint (LogRecord logRecord, Set endTransactions) {
        // 读取endCheckpoint记录的表并与memory中的当前表融合
        if (logRecord.getType().equals(LogType.END_CHECKPOINT)) {
            // update ATT
            Map<Long, Pair<Transaction.Status, Long>> checkpointTransactionTable = logRecord.getTransactionTable();
            for (Long tranNum : checkpointTransactionTable.keySet()) {
                if (endTransactions.contains(tranNum)) {
                    continue;
                }
                Pair<Transaction.Status, Long> pair = checkpointTransactionTable.get(tranNum);
                if (!transactionTable.containsKey(tranNum)) {
                    startTransaction(newTransaction.apply(tranNum));
                    transactionTable.get(tranNum).lastLSN = pair.getSecond();
                }
                // update the lastLSN
                transactionTable.get(tranNum).lastLSN = Math.max(transactionTable.get(tranNum).lastLSN, pair.getSecond());
                // update the Transaction Status if is more advanced than what we have in memory
                // 检查是否有必要更新事务状态, 有则更新. 注意ABORTING要改为RECOVERY_ABORTING, Ended的事务要让它结束掉
                if (transactionStatusBefore(transactionTable.get(tranNum).transaction.getStatus(), pair.getFirst())) {
                    if (pair.getFirst().equals(Transaction.Status.ABORTING)) {
                        // 就是把Aborting换成Recovery_aborting
                        transactionTable.get(tranNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    } else {
                        transactionTable.get(tranNum).transaction.setStatus(pair.getFirst());
                    }
                }
            }

            // update DPT
            Map<Long, Long> checkpointDirtyPageTable = logRecord.getDirtyPageTable();
            for (Long tranNum : checkpointDirtyPageTable.keySet()) {
                // 因为checkpoing记录的recLSN一定是最早的
                if (!dirtyPageTable.containsKey(tranNum)) {
                    dirtyPageTable.put(tranNum, checkpointDirtyPageTable.get(tranNum));
                } else {
                    dirtyPageTable.put(tranNum, Math.min(checkpointDirtyPageTable.get(tranNum), dirtyPageTable.get(tranNum)));
                }

            }

        }
    }

    /**
     * util for judging the before and after for the Transaction Status
     * @param status1
     * @param status2
     * @return status1 < status2
     */
    private boolean transactionStatusBefore (Transaction.Status status1, Transaction.Status status2) {
        if (status1.equals(Transaction.Status.RUNNING)) {
            return status2.equals(Transaction.Status.COMMITTING) || status2.equals(Transaction.Status.COMPLETE)|| status2.equals(Transaction.Status.ABORTING);
        } else if (status1.equals(Transaction.Status.COMMITTING) || status1.equals(Transaction.Status.ABORTING)) {
            return status2.equals(Transaction.Status.COMPLETE);
        } else {
            return false;
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a partition (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        Optional<Long> minRecLsn = dirtyPageTable.values().stream().min(Long::compareTo);
        if(!minRecLsn.isPresent()) {
            // 除非DPT是空的...那就不用遍历了
            return;
        }
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(minRecLsn.get());
        while (logRecordIterator.hasNext()) {
            LogRecord logRecord = logRecordIterator.next();
            if (logRecord.isRedoable()) {
                if (logModifyPages(logRecord.getType())) {
                    assert logRecord.getPageNum().isPresent();
                    Long pageNum = logRecord.getPageNum().get();
                    // 要保证DPT中有Page且recLSN <= LSN
                    if (!(dirtyPageTable.containsKey(pageNum) && dirtyPageTable.get(pageNum) <= logRecord.getLSN())) continue;
                    // 不需要考虑并发问题, 可以用一个DummyLockContext
                    // no other operations can run at the same time as the redo phase
                    Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                    long pageLSN;
                    try {
                       pageLSN = page.getPageLSN();
                    } finally {
                        page.unpin();
                    }
                    // 判断当前LSN是否新于PageLSN
                    if (!(logRecord.getLSN() > pageLSN)) continue;
                }
                logRecord.redo(this, diskSpaceManager, bufferManager);
            }
        }
    }

    /**
     * util method to judge whether the Type of log is to modify a page
     */
    private boolean logModifyPages(LogType logType) {
        return logType.equals(LogType.UPDATE_PAGE) || logType.equals(LogType.UNDO_UPDATE_PAGE)
                || logType.equals(LogType.UNDO_ALLOC_PAGE) || logType.equals(LogType.FREE_PAGE);
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and emit the appropriate CLR
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if not available) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        Queue<Pair<Long, Long>> undoLSNs = new PriorityQueue<>(new PairFirstReverseComparator<>());
        for (Long transNum : transactionTable.keySet()) {
            if (transactionTable.get(transNum).transaction.getStatus().equals(Transaction.Status.RECOVERY_ABORTING)) {
                Long LSN = transactionTable.get(transNum).lastLSN;
                LogRecord logRecord = logManager.fetchLogRecord(LSN);
                if (logRecord.getType().equals(LogType.UNDO_UPDATE_PAGE)) {
                    LSN = logRecord.getUndoNextLSN().isPresent() ? logRecord.getUndoNextLSN().get() : null;
                }
                // null可能是已经undo完了没有停下事务也可能是空事务...
                if(LSN != null) undoLSNs.add(new Pair<>(LSN, transNum));
            }
        }
        while (!undoLSNs.isEmpty()) {
            Pair<Long, Long> poll = undoLSNs.poll();
            Long LSN = poll.getFirst();
            Long transNum = poll.getSecond();
            LogRecord logRecord = logManager.fetchLogRecord(LSN);
            if (logRecord.isUndoable()) {
                LogRecord CLR = logRecord.undo(transactionTable.get(transNum).lastLSN);
                logManager.appendToLog(CLR);
                transactionTable.get(transNum).lastLSN = CLR.LSN;
                CLR.redo(this, diskSpaceManager, bufferManager); // CLR的redo是我们的undo
            }

            Transaction transaction = transactionTable.get(transNum).transaction;
            if (!logRecord.getPrevLSN().isPresent() || logRecord.getPrevLSN().get() == 0) {
                // End the transaction if the LSN from the previous step is 0
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(transaction.getTransNum(), transactionTable.get(transNum).lastLSN));
                transactionTable.remove(transNum);
            } else {
                // 把下一条加入Queue
                undoLSNs.add(new Pair<>(logRecord.getPrevLSN().get(), transNum));
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
