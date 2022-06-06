package edu.berkeley.cs186.database.concurrency;

public enum IsolationLevel {
    REPEATABLE_READ, // 可重复读
    READ_COMMITTED, // 读提交
    READ_UNCOMMITTED // 读未提交
}
