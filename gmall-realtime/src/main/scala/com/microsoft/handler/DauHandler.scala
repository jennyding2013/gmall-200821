package com.microsoft.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.microsoft.bean.StartUpLog
import com.microsoft.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author Jenny.D
 * @create 2021-01-07 15:45
 */
object DauHandler {
  def filterByMid(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    val dateMidToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => ((log.logDate,log.mid),log))

    val dateMidToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()

   /* val value: DStream[((String, String), List[StartUpLog])] = dateMidToLogIterDStream.mapValues(iter=>{iter.toList.sortWith(_.ts < _.ts).take(1)})

      val value1: DStream[List[StartUpLog]] = value.map(_._2)
    val value2: DStream[StartUpLog] = value1.flatMap(x=>x)
   value2*/

      val value3: DStream[StartUpLog] = dateMidToLogIterDStream.flatMap{case((date,mid),iter)=>iter.toList.sortWith(_.ts<_.ts).take(1)}

      value3
  }


  def saveMidToRedis(startLogDStream:DStream[StartUpLog]):Unit={
    startLogDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(iter=>{

          val jedisClient: Jedis = RedisUtil.getJedisClient

          iter.foreach(log=>{
            val redisKey = s"DAU:${log.logDate}"
            jedisClient.sadd(redisKey,log.mid)

          })
          jedisClient.close()

        }
        )
      }
    )

  }

  def filterByRedis(startLogDStream:DStream[StartUpLog],sc:SparkContext):DStream[StartUpLog]={
    //方案一：
/*    val value1: DStream[StartUpLog] = startLogDStream.filter(startUpLog => {

      val jedisClient: Jedis = RedisUtil.getJedisClient
      val redisKey = s"DAU:${startUpLog.logDate}"
      val exist: lang.Boolean = jedisClient.sismember(redisKey, startUpLog.mid)
      jedisClient.close()
      !exist
    })
    value1*/

    //方案二：使用MapPartitions代替filter,减少连接的创建和释放
  /*  val value2: DStream[StartUpLog] = startLogDStream.mapPartitions(iter => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val logs: Iterator[StartUpLog] = iter.filter(startUpLog => {
        val redisKey = s"DAU:${startUpLog.logDate}"
        !jedisClient.sismember(redisKey, startUpLog.mid)

      })

      jedisClient.close()
      logs
    }
    )
    value2
*/

    //方案三：每个批次获取一次连接，访问一次Redis,使用广播变量发送至Executor
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val value3: DStream[StartUpLog] = startLogDStream.transform(
      rdd => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val redisKey = s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}"
        val mids: util.Set[String] = jedisClient.smembers(redisKey)

        jedisClient.close()

        val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
        rdd.filter(startLog => {
          !midsBC.value.contains(startLog.mid)
        })
      })

     value3

  }

}
