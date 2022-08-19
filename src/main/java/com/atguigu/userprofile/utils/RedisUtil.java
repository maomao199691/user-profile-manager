package com.atguigu.userprofile.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

@Configuration
public class RedisUtil {

    public static void main(String[] args) {

    }
    public static String redisHost;

    public  static Integer redisPort;

    @Value("${spring.redis.host}")
    public void setRedisHost(String redisHost){
        RedisUtil.redisHost=redisHost;
    }
    @Value("${spring.redis.port}")
    public void setRedisPort(String redisPort){
        RedisUtil.redisPort=Integer.valueOf(redisPort);
    }


    private static  JedisPool  jedisPool =null;

    public static Jedis getJedis() {

      if(jedisPool==null){
          JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
          jedisPoolConfig.setMaxTotal(200); // 最大连接数
          jedisPoolConfig.setMaxIdle(30);// 最多维持30
          jedisPoolConfig.setMinIdle(10);// 至少维持10
          jedisPoolConfig.setBlockWhenExhausted(true);
          jedisPoolConfig.setMaxWaitMillis(5000);
          jedisPoolConfig.setTestOnBorrow(true); //借走连接时测试

          jedisPool = new JedisPool(jedisPoolConfig,redisHost,redisPort,60000);

      }
       return   jedisPool.getResource();
    }
}
