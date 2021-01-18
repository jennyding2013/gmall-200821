package com.microsoft.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author Jenny.D
 * @create 2021-01-06 21:15
 */


object RedisUtil {
    var jedisPool:JedisPool = _
  def getJedisClient:Jedis={
    if(jedisPool == null){
      println("开辟一个连接池")
      val config: Properties = PropertiesUtil.load("config.properties")
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)
      jedisPoolConfig.setMaxIdle(20)
      jedisPoolConfig.setMinIdle(20)
      jedisPoolConfig.setBlockWhenExhausted(true)
      jedisPoolConfig.setMaxWaitMillis(500)
      jedisPoolConfig.setTestOnBorrow(true)

     jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)

    }

    //println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    //  println("获取一个连接")
    jedisPool.getResource

  }
}

