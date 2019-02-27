package com.yujinyi.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
/**
 * 锁的基础操作
 * @author Administrator
 *
 */
public interface RedisLock {
	/**
	 * 获取锁成功后锁住资源
	 */
    void lock() throws InterruptedException, TimeoutException;
    
    /**
	 * 获取锁成功后锁住资源（可中断）
	 */
    void lockInterruptibly() throws InterruptedException, TimeoutException;
    
    /**
	 * 尝试获取锁
	 */
    boolean tryLock();

    /**
     * 尝试获取锁（可超时）
     * @param time 时间
     * @param unit 时间单位
     */
    boolean tryLock(long time, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
	 * 释放锁
	 */
    void unlock();

    /**
     * 等待锁的条件
     */
    Condition newCondition();
}
