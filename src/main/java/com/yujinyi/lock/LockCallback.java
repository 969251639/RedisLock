package com.yujinyi.lock;

/**
 * 超时回调方法
 *
 */
public interface LockCallback {
	/**
	 * 获取不到锁时的回调方法
	 */
	public void callback(String key);
}
