package org.apache.flink.utils;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolFactory {

    private static volatile JedisPool jedisPool = null;
    private static int port;
    private static String ip;

    public static void init(String redisIp, int redisPort) {
        ip = redisIp;
        port = redisPort;
    }

    public static JedisPool getInstance() {
        if (jedisPool == null) {
            synchronized (JedisPoolFactory.class) {
                if (jedisPool == null) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(100);
                    config.setMinIdle(10);
                    config.setMaxIdle(10);
                    config.setMaxWaitMillis(2000);
                    config.setTestWhileIdle(false);
                    config.setTestOnBorrow(false);
                    config.setTestOnReturn(false);
                    jedisPool = new JedisPool(config, ip, port);
                }
            }

        }
        return jedisPool;

    }
}
