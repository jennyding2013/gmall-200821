package com.microsoft.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.microsoft.bean.StartUpLog
import com.microsoft.constants.GmallConstant
import com.microsoft.handler.DauHandler
import com.microsoft.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author Jenny.D
 * @create 2021-01-07 12:23
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP, ssc)

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
      val ts: Long = startUpLog.ts
      val dateHourStr: String = sdf.format(new Date(ts))
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      startUpLog
    })

    startLogDStream.cache()
    startLogDStream.count().print()

    // startLogDStream.print()

  val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startLogDStream,ssc.sparkContext)
    filterByRedisDStream.cache()
    filterByRedisDStream.count().print()


  val filterByMidDStream: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDStream)

  filterByMidDStream.cache()
    filterByMidDStream.count().print()

    DauHandler.saveMidToRedis(filterByMidDStream)

    filterByMidDStream.foreachRDD(rdd=>{
        rdd.saveToPhoenix("GMALL2020_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
    })


    ssc.start()
    ssc.awaitTermination()
  }
}




