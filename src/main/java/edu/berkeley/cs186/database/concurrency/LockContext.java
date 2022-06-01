package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    // LockContext可以是只读的... 就不能调用acquire等方法了. 真奇怪
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    // transaction id 到 对应LockContext的子树的锁的数量的映射
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name); // ResourceName的层级关系直接体现在Name中...
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        // 需要从根节点迭代遍历得到对应的Context
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        // 用这个方法创建的应该就是没有parent的根节点
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (transaction == null) return;

        if (this.readonly) {
            throw new UnsupportedOperationException("LockContext is readonly");
        }

        if (this.lockman.getLockType(transaction, this.getResourceName()).equals(lockType)) {
            throw new DuplicateLockRequestException("lock is already held");
        }

        if (parent != null && !LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)) {
            throw new InvalidLockException("request violates multigranularity locking constraints");
        }

        // 所有检查都通过了
        lockman.acquire(transaction, this.getResourceName(), lockType);
        // 更新父节点numChildLocks
        this.updateAncestorNumChildren(transaction.getTransNum(), 1);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (transaction == null) return;
        if (this.readonly) {
            throw new UnsupportedOperationException("LockContext is readonly");
        }

        if (this.getExplicitLockType(transaction).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock was hold by current transaction");
        }

        // 我觉着吧, 只要子节点有锁, 父节点就不应该解锁
        if (this.numChildLocks.containsKey(transaction.getTransNum()) && this.numChildLocks.get(transaction.getTransNum()) > 0) {
            throw new InvalidLockException("release violates multigranularity locking constraints");
        }

        lockman.release(transaction, this.getResourceName());
        // 更新parent中numChildLocks内容
        this.updateAncestorNumChildren(transaction.getTransNum(), -1);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        LockType oldLockType = this.getExplicitLockType(transaction);

        if (this.readonly) {
            throw new UnsupportedOperationException("LockContext is readonly");
        }

        if (oldLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock was hold by current transaction");
        }

        if (oldLockType.equals(newLockType)) {
            throw new DuplicateLockRequestException("Duplicate Lock Request");
        }

        // 开始处理invalidLockException
        // case 1 not a promotion
        if  (!LockType.substitutable(newLockType, oldLockType)) {
            throw new InvalidLockException("not a promote");
        }

        // case 2 promoting cause invalid case
        if (parent != null && !LockType.canBeParentLock(this.parent.getExplicitLockType(transaction), newLockType)) {
            throw new InvalidLockException("promotion causes invalid state");
        }

        // 可以promote了
        lockman.promote(transaction, this.getResourceName(), newLockType);
        if (newLockType.equals(LockType.SIX)) {
            // 释放所有子节点的 IS/S
            List<ResourceName> sisDescendants = this.sisDescendants(transaction);
            for (ResourceName resourceName : sisDescendants) {
                lockman.release(transaction, resourceName);
            }
            updateAncestorNumChildren(transaction.getTransNum(), -sisDescendants.size());
        }



        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        // 突然意识到之前写的部分是有问题的, 必须递归地把所有后继节点的锁都释放掉
        LockType oldLock = this.getExplicitLockType(transaction);

        if (this.readonly) {
            throw new UnsupportedOperationException("LockContext is readonly");
        }

        if (oldLock.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock hold by current transaction");
        }

        List<ResourceName> releaseNames = new ArrayList<>();
        switch (oldLock) {
            case SIX:
            case IX:
                // 要升级到X
                releaseNames = this.lockedDescendants(transaction);
                lockman.acquireAndRelease(transaction, this.getResourceName(), LockType.X, releaseNames);
                break;
            case IS:
                // 需要释放子节点中所有的S/IS节点
                releaseNames = this.sisDescendants(transaction);
                lockman.acquireAndRelease(transaction, this.getResourceName(), LockType.S, releaseNames);
                break;
            default:
                break;
        }
        this.updateAncestorNumChildren(transaction.getTransNum(), -releaseNames.size());
        this.numChildLocks.putIfAbsent(transaction.getTransNum(), 0);
        this.numChildLocks.put(transaction.getTransNum(), 0);

    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, this.getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     * 就是说父节点是S, 那么子节点的当前方法也要返回S
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType lockType = lockman.getLockType(transaction, this.getResourceName());
        if ((lockType.equals(LockType.NL) || lockType.isIntent()) && parent != null) {
            LockType parentEffectiveLockType = parent.getEffectiveLockType(transaction);

            // 根据父节点持有锁的情况来判断
            if (parentEffectiveLockType.equals(LockType.S) || parentEffectiveLockType.equals(LockType.X)) {
                return parentEffectiveLockType;
            }

            if (hasSIXAncestor(transaction)) {
                return LockType.S;
            }
        }
        return lockType;
    }


    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (this.parent == null) return false;
        LockContext parent = this.parent;
        while (parent != null) {
            if (parent.getEffectiveLockType(transaction).equals(LockType.SIX)) {
                return true;
            }
            parent = parent.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> sisDescendants = new ArrayList<>();
        for (LockContext lockContext : children.values()) {

            // 递归调用得到子节点的后继节点信息
            List<ResourceName> childsisDescendants = lockContext.sisDescendants(transaction);
            for (ResourceName resourceName : childsisDescendants) {
                sisDescendants.add(resourceName);
            }

            if (lockContext.getExplicitLockType(transaction).equals(LockType.S)
                    || lockContext.getExplicitLockType(transaction).equals(LockType.IS)) {
                sisDescendants.add(lockContext.getResourceName());
            }
        }
        return sisDescendants;
    }

    /**
    * Helper method to get a list of resourceNames of all locks
     *  and are descendants of current context for the given transaction.
    */
    private List<ResourceName> lockedDescendants (TransactionContext transaction) {
        List<ResourceName> lockedDescendants = new ArrayList<>();
        for (LockContext lockContext : children.values()) {

            // 递归调用得到子节点的后继节点信息
            List<ResourceName> childlockedDescendants = lockContext.lockedDescendants(transaction);
            for (ResourceName resourceName : childlockedDescendants) {
                lockedDescendants.add(resourceName);
            }

            if (!lockContext.getExplicitLockType(transaction).equals(LockType.NL)) {
                lockedDescendants.add(lockContext.getResourceName());
            }
        }

        return lockedDescendants;
    }

    /*
    * 更新所有祖节点的numChildLocks
    * */
    private void updateAncestorNumChildren (Long transactionId, int delta) {
        if (this.parent == null) return;
        if (this.parent.parent != null) this.parent.updateAncestorNumChildren(transactionId, delta);
        this.parent.numChildLocks.putIfAbsent(transactionId, 0);
        this.parent.numChildLocks.put(transactionId, this.parent.numChildLocks.get(transactionId) + delta);
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp); // 返回之前name对应的值, 不存在则返回null
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

