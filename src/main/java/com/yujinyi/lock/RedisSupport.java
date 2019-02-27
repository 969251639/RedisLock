package com.yujinyi.lock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisSupport {
	private static JedisPool jedisPool = null;
	private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();
	public static final LockSubscribe LOCK_SUBSCRIBE = new LockSubscribe();
	
	private static JedisPool getJedisPool() {
		if(jedisPool == null) {
			throw new NullPointerException("jedis pool 不能为空");
		}
		EXECUTOR.execute(() -> {
			Jedis jedis = RedisSupport.getJedis();
	    	try {
	    		jedis.psubscribe(LOCK_SUBSCRIBE, RedisDistributedLock.CHANNEL_PREFIX + ":*");
	    	}finally {
	    		RedisSupport.close(jedis);
	    	}
		});
		return jedisPool;
	}
	
	public static void setJedisPool(JedisPool jedisPool) {
		RedisSupport.jedisPool = jedisPool;
	}

	public static Jedis getJedis() {
		return getJedisPool().getResource();
	}
	
	public static void close(Jedis jedis) {
		jedis.close();
	}
}
