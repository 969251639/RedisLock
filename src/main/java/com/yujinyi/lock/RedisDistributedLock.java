package com.yujinyi.lock;

import java.util.Arrays;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Redis实现分布式锁(基于juc.lock实现)
 * @author Administrator
 *
 */
public class RedisDistributedLock implements RedisLock {
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisDistributedLock.class);
	private final LockSubscribe subscriber = RedisSupport.LOCK_SUBSCRIBE;
	private LockCallback lockTimeoutCallback;
	private int delayExecutedCount = 0;
	private int delayCount = 0;//延长器的执行次数，默认为0：表示无数次
	
	/**
	 * 竞争消息通道
	 */
	public static final String CHANNEL_PREFIX = "lock__channel";

    /**
     * 默认请求锁的超时时间(ms 毫秒)
     */
	public static final long DEFAULT_TIME_OUT = 1000;

    /**
     * 默认锁的有效时间(s)
     */
	public static final int DEFAULT_EXPIRE_TIME = 60;

    /**
     * 锁标志对应的key
     */
    private String lockKey;

    /**
     * 锁对应的值
     */
    private String lockValue = UUID.randomUUID().toString() + ":" + Thread.currentThread().getId();

    /**
     * 锁的有效时间(s)
     */
    private int expireTime = DEFAULT_EXPIRE_TIME;

    /**
     * 请求锁的超时时间(ms)
     */
    private long timeOut = DEFAULT_TIME_OUT;
    
    /**
     * 延长执行时间计时器
     */
    private Timer timer = new Timer();
    
    /**
     * 加锁LUA脚本
     */
    public static final String LOCK_SCRIPT = 
    	"if (redis.call('exists', KEYS[1]) == 0) then " +
    		"redis.call('hset', KEYS[1], ARGV[2], 1); " +
    		"redis.call('expire', KEYS[1], ARGV[1]); " +
    		"return nil; " +
    	"end; " +
    	"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
    		"redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
    		"redis.call('expire', KEYS[1], ARGV[1]); " +
    		"return nil; " +
    	"end; " +
    	"return redis.call('pttl', KEYS[1]);";
    
    /**
     * 解锁LUA脚本
     */
    public static final String UNLOCK_SCRIPT = 
		"if (redis.call('exists', KEYS[1]) == 0) then " +
	        "redis.call('publish', KEYS[2], ARGV[1]); " +
	        "return 1; " +
	    "end;" +
	    "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
	        "return nil;" +
	    "end; " +
	    "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
	    "if (counter > 0) then " +
	        "redis.call('expire', KEYS[1], ARGV[2]); " +
	        "return 0; " +
	    "else " +
	        "redis.call('del', KEYS[1]); " +
	        "redis.call('publish', KEYS[2], ARGV[1]); " +
	        "return 1; "+
	    "end; " +
	    "return nil;";
    
    /**
     * 强制解锁LUA脚本
     */
    public static final String FORCE_UNLOCK_SCRIPT = 
    	"if (redis.call('del', KEYS[1]) == 1) then " + 
    		"redis.call('publish', KEYS[2], ARGV[1]); " + 
    		"return 1; " + 
        "else " + 
        	"return 0; " + 
        "end";
    
    public static final String RENEWAL_EXPIRETION_SCRIPT = 
    	"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
    		"redis.call('expire', KEYS[1], ARGV[1]); " +
    		"return 1; " +
    	"end; " +
    	"return 0;";

	/**
     * 使用默认的锁过期时间和请求锁的超时时间
     *
     * @param lockKey       锁的key（Redis的Key）
     */
    public RedisDistributedLock(String lockKey) {
    	this.lockKey = lockKey;
    }

    /**
     * 使用默认的请求锁的超时时间，指定锁的过期时间
     *
     * @param lockKey       锁的key（Redis的Key）
     * @param expireTime    锁的过期时间(单位：秒)
     */
    public RedisDistributedLock(String lockKey, int expireTime) {
    	this.lockKey = lockKey;
    	this.expireTime = expireTime;
    }

    /**
     * 使用默认的锁的过期时间，指定请求锁的超时时间
     *
     * @param lockKey       锁的key（Redis的Key）
     * @param timeOut       请求锁的超时时间(单位：毫秒)
     */
    public RedisDistributedLock(String lockKey, long timeOut) {
    	this.lockKey = lockKey;
    	this.timeOut = timeOut;
    }

    /**
     * 锁的过期时间和请求锁的超时时间都是用指定的值
     *
     * @param lockKey       锁的key（Redis的Key）
     * @param expireTime    锁的过期时间(单位：秒)
     * @param timeOut       请求锁的超时时间(单位：毫秒)
     */
    public RedisDistributedLock(String lockKey, int expireTime, long timeOut) {
        this.lockKey = lockKey;
        this.expireTime = expireTime;
    	this.timeOut = timeOut;
    }

    /**
     * 上锁
     */
	@Override
	public void lock() throws InterruptedException, TimeoutException {
		lockInterruptibly();
	}

	/**
     * 上锁(可中断)
     */
	@Override
	public void lockInterruptibly() throws InterruptedException, TimeoutException {
		try {
			if(!tryLock(timeOut, TimeUnit.MILLISECONDS)) {
				if(lockTimeoutCallback != null) {
					lockTimeoutCallback.callback(lockKey);
				}
				throw new TimeoutException(String.format("获取锁超时, key: %s, value: %s", lockKey, lockValue));
			}
		}catch(TimeoutException timeoutException) {
			throw new TimeoutException(String.format("获取锁超时, key: %s, value: %s", lockKey, lockValue));
		}catch(InterruptedException interruptedException) {
			throw new TimeoutException(String.format("获取锁的线程已中断, key: %s, value: %s", lockKey, lockValue));
		}
	}

	/**
	 * 尝试上锁
	 */
	@Override
	public boolean tryLock() {
		return tryAcquire() == null;
	}

	/**
     * 尝试上锁
     * @param waitTime  等待的超时时间
     * @param unit      时间单位
     */
	@Override
	public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException, TimeoutException {
		String channel = getChannelName();
		long time = unit.toMillis(waitTime);
		long currentTime = System.currentTimeMillis();
		Long result = tryAcquire();//尝试获取锁
		if(result == null) {//获取成功
			return true;
		}
		time = time - (System.currentTimeMillis() - currentTime);
        if (time <= 0) {//超时返回失败
            return false;
        }
        
        //订阅争锁通道
        currentTime = System.currentTimeMillis();
        subscriber.subscribe(channel);//订阅
        
        try {
	        for(;;) {
	        	time = time - (System.currentTimeMillis() - currentTime);
	            if (time <= 0) {//超时返回失败
	                return false;
	            }
	        	//订阅争锁通道
	            currentTime = System.currentTimeMillis();
	            result = tryAcquire();//尝试再获取一次
	            if (result == null) {
	                return true;
	            }
	
	            time = time - (System.currentTimeMillis() - currentTime);
	            if (time <= 0) {//超时返回失败
	                return false;
	            }
	
	            //等待消息通知后重新竞争锁
	            currentTime = System.currentTimeMillis();
	            if (result >= 0 && result < time) {
	            	subscriber.wait(channel, result, TimeUnit.MILLISECONDS);
	            } else {
	            	subscriber.wait(channel, time, TimeUnit.MILLISECONDS);
	            }
	        }
        } finally {
        	subscriber.unsubscribe(channel);//退订
        }
		
	}

	/**
	 * 解锁
	 */
	@Override
	public void unlock() {
		Jedis jedis = RedisSupport.getJedis();
    	try {
			//返回null则unlock的线程非持有锁的线程
			//返回1则锁不存在或者释放成功，则发消息通知其他等待线程竞争锁
			//返回0则重入次数非空，延长锁的超时时间
	        Object obj = jedis.eval(UNLOCK_SCRIPT, Arrays.<String>asList(lockKey, getChannelName()), 
	        		Arrays.<String>asList(LockSubscribe.UNLOCK_MESSAGE.toString(), String.valueOf(expireTime), lockValue));
			Long result = (Long)obj;
	        if(result == null) {
	        	LOGGER.info("解锁线程与持有锁的线程不一致，解锁失败, key: {}, value: {}", lockKey, lockValue);
	        	throw new IllegalMonitorStateException("解锁线程与持有锁的线程不一致，解锁失败");
	        }else if(result == 1L) {
	        	LOGGER.info("解锁成功, key: {}, value: {}", lockKey, lockValue);
	        	//移除延长key定时器 
	        	cancelExpiration();
	        }else if(result == 0L) {
	        	LOGGER.info("重入次数减一, key: {}, value: {}", lockKey, lockValue);
	        }
    	}finally {
    		RedisSupport.close(jedis);
    	}
	}
	
	/**
	 * 强制解锁
	 */
	public void forceUnlock() {
		Jedis jedis = RedisSupport.getJedis();
    	try {
			Object obj = jedis.eval(FORCE_UNLOCK_SCRIPT, Arrays.<String>asList(lockKey, getChannelName()), Arrays.<String>asList(LockSubscribe.UNLOCK_MESSAGE.toString()));
			Long result = (Long)obj;
			if(result == 1L) {
				LOGGER.info("强制解锁成功, key: {}, value: {}", lockKey);
			}else {
				LOGGER.info("强制解锁失败, key: {}, value: {}", lockKey);
			}
    	}finally {
    		RedisSupport.close(jedis);
    	}
	}
	
	/**
	 * 获取重入次数
	 */
	public int getHoldCount() {
		Jedis jedis = RedisSupport.getJedis();
    	try {
    		return Integer.parseInt(jedis.hget(lockKey, lockValue));
    	}finally {
    		RedisSupport.close(jedis);
    	}
	}
	
	/**
	 * 是否被锁住
	 */
	public boolean isLocked() {
		Jedis jedis = RedisSupport.getJedis();
    	try {
    		return jedis.exists(lockKey);
    	}finally {
    		RedisSupport.close(jedis);
    	}
	}

	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 申请锁
	 */
	private Long tryAcquire() {
		Jedis jedis = RedisSupport.getJedis();
    	try {
			Object obj = jedis.eval(LOCK_SCRIPT, Collections.<String>singletonList(lockKey), Arrays.<String>asList(String.valueOf(expireTime), lockValue));
			if(obj == null) {
				renewalExpiration();
			}
	        return (Long)obj;
    	}finally {
    		RedisSupport.close(jedis);
    	}
	}
	
	/**
	 * 延长器
	 */
	private void renewalExpiration() {
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				Jedis jedis = RedisSupport.getJedis();
		    	try {
					jedis.eval(RENEWAL_EXPIRETION_SCRIPT, Collections.<String>singletonList(lockKey), Arrays.<String>asList(String.valueOf(expireTime), lockValue));
					delayExecutedCount++;
		    	}finally {
		    		RedisSupport.close(jedis);
		    	}
			}
		}, (expireTime / 3) * 1000, (expireTime / 3) * 1000);
		if(delayCount > 0 && delayCount > delayExecutedCount) {
			cancelExpiration();
		}
	}
	
	/**
	 * 取消延长定时器
	 */
	private void cancelExpiration() {
		timer.cancel();
	}
	
	private String getChannelName() {
		return CHANNEL_PREFIX + ":" + lockKey;
	}

	public String getLockKey() {
		return lockKey;
	}

	public String getLockValue() {
		return lockValue;
	}

	public int getExpireTime() {
		return expireTime;
	}

	public long getTimeOut() {
		return timeOut;
	}

	public void setLockTimeoutCallback(LockCallback lockTimeoutCallback) {
		this.lockTimeoutCallback = lockTimeoutCallback;
	}

	public void setDelayCount(int delayCount) {
		this.delayCount = delayCount;
	}
	
}
