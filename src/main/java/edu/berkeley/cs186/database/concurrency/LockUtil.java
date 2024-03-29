package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL, 不可以是意向锁
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // 检查当前隔离等级, 若为read uncommitted则不加S锁
        if (Database.DBisolationLevel.equals(IsolationLevel.READ_UNCOMMITTED) && requestType.equals(LockType.S)) {
            return;
        }

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction(); // 得到当前正在运行的事务
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // 检查是否已经持有锁
        if (effectiveLockType.equals(requestType) || LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
        // 当前锁是IX并且需要申请S, 要升级到SIX
        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            // 首先检查父节点是否满足条件
            if (!(parentContext.getEffectiveLockType(transaction).equals(LockType.SIX)
                    || parentContext.getEffectiveLockType(transaction).equals(LockType.IX))) {
                addIntentLock(transaction, lockContext, LockType.IX);
            }
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        // 当前锁是意向锁
        // 可能的情况: IS -> S / X, IX -> X, SIX -> X
        // 就是escalate的情况
        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction); // 可以满足IS -> S, IX -> X, SIX -> X且父节点限制可以被满足
            if (!(requestType.equals(LockType.X) && explicitLockType.equals(LockType.IS))) {
                return;
            }
        }
        // 当前锁是S或者NL, 要升级到X或者S/X
        if (explicitLockType.equals(LockType.S)) {
            // S -> X
            if (parentContext!= null && !LockType.canBeParentLock(parentContext.getExplicitLockType(transaction), requestType)) {
                addIntentLock(transaction, parentContext, LockType.IX);
            }
            lockContext.promote(transaction, requestType);
        } else {
            // NL -> S / X
            if (parentContext != null && !LockType.canBeParentLock(parentContext.getExplicitLockType(transaction), requestType)) {
                LockType intentLockType = requestType.equals(LockType.X) ? LockType.IX : LockType.IS;
                addIntentLock(transaction, parentContext, intentLockType);
            }
            lockContext.acquire(transaction, requestType);
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    // 给LockContext加意向锁
    // 一般用于父节点首先去获取意向锁
    private static void addIntentLock (TransactionContext transaction, LockContext lockContext, LockType lockType) {
        LockContext parent = lockContext.parentContext();
        if (parent != null) {
            LockType parentLockType = parent.getEffectiveLockType(transaction);
            switch (lockType) {
                case IS:
                    if (parentLockType.equals(LockType.NL)) {
                        addIntentLock(transaction, parent, LockType.IS);
                    }
                    break;
                case IX:
                    if (parentLockType.equals(LockType.NL) || parentLockType.equals(LockType.IS)) {
                        addIntentLock(transaction, parent, LockType.IX);
                    }
                    break;
                default:
                    throw new InvalidLockException("parentContext only allow intent locks");
            }
        }
        // 原来的锁只可能是S, IS, NL. 不可能是X, IX, SIX, 不然直接加就行了
        if (lockContext.getExplicitLockType(transaction).equals(LockType.NL)) lockContext.acquire(transaction, lockType); // 原来没有锁
        else if (lockContext.getExplicitLockType(transaction).equals(LockType.S) && lockType.equals(LockType.IX)) {
            // S -> IX的过程, 需要变成SIX
            lockContext.promote(transaction, LockType.SIX);
        } else {
            // IS -> IX
            lockContext.promote(transaction, lockType);
        }
    }

    // 在read committed下S锁在使用之后要立即释放
    public static void releaseSIfNecessary (LockContext lockContext) {
        if (Database.DBisolationLevel.equals(IsolationLevel.READ_COMMITTED)) {
            // Do nothing if the transaction or lockContext is null
            TransactionContext transaction = TransactionContext.getTransaction(); // 得到当前正在运行的事务
            if (transaction == null | lockContext == null) return;
            // 如果事务持有非S锁则直接返回
            if (!(lockContext.getExplicitLockType(transaction).equals(LockType.S))) return;
            lockContext.release(transaction);
            // 递归释放父节点的IS锁
            LockContext parentContext = lockContext.parentContext();
            while (parentContext != null && parentContext.getNumChildren(transaction) == 0) {
                // 一定是IS或者IX锁
                assert parentContext.getExplicitLockType(transaction).isIntent();
                parentContext.release(transaction);
                parentContext = parentContext.parent;
            }
        }
    }
}
