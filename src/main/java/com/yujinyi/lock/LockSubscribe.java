package com.yujinyi.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import redis.clients.jedis.JedisPubSub;

/**
 * 锁事件订阅
 * 当有监听到锁的释放后通知其他所有的节点的客户端可以重新抢锁
 * @author Administrator
 *
 */
public class LockSubscribe extends JedisPubSub {
	public static final Long UNLOCK_MESSAGE = 0L;
	private static final Map<String, Semaphore> MAP = new ConcurrentHashMap<>();
	
	@Override
	public void onPMessage(String pattern, String channel, String message) {
        if(UNLOCK_MESSAGE.toString().equals(message)) {//收到解锁消息后释放信号量
        	release(channel);
        }
    }
	
	public void subscribe(String channel) {
		if(!MAP.containsKey(channel)) {//给锁住的通道绑定信号量
			MAP.put(channel, new Semaphore(0));
		}
	}

	public void unsubscribe(String channel) {
		MAP.remove(channel);
	}
	
	/**
	 * 线程等待休眠直至调用release方法释放信号量
	 */
	public void wait(String channel, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
		Semaphore semaphore = MAP.get(channel);
		if(semaphore != null) {
			semaphore.tryAcquire(timeout, unit);
		}
	}
	
	/**
	 * 释放信号量，唤醒线程重新争抢锁
	 */
	public void release(String channel) {
		Semaphore semaphore = MAP.get(channel);
		if(semaphore != null) {
			semaphore.release(semaphore.getQueueLength());
		}
	}
}
