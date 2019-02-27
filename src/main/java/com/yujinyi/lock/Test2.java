package com.yujinyi.lock;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Hello world!
 *
 */
public class Test2 {
	public static void main(String[] args) {
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxIdle(5);
		jedisPoolConfig.setMaxWaitMillis(2000);
		jedisPoolConfig.setMaxTotal(10);
		jedisPoolConfig.setMinIdle(1);
		jedisPoolConfig.setTestOnBorrow(true);
		jedisPoolConfig.setTestOnReturn(true);
		JedisPool jedisPool = new JedisPool(jedisPoolConfig, "119.23.246.236", 6379, 5000, "xty-test-123");
		RedisSupport.setJedisPool(jedisPool);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		RedisDistributedLock lock = new RedisDistributedLock("userId", 10000L);
		try {
			System.out.println("begin time: " + simpleDateFormat.format(new Date()));
			lock.lock();
			System.out.println("workid: test2");
			Thread.sleep(5000);
			System.out.println("end time: " + simpleDateFormat.format(new Date()));
		} catch (InterruptedException | TimeoutException e) {
			e.printStackTrace();
		}finally {
			lock.unlock();
			System.exit(0);
		}
	}
}
