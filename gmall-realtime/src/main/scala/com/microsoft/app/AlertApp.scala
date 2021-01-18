package com.microsoft.app

import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.microsoft.bean.{CouponAlertInfo, EventLog}
import com.microsoft.constants.GmallConstant
import com.microsoft.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * @author Jenny.D
 * @create 2021-01-12 20:56
 */
object AlertApp {
  def main(args:Array[String]):Unit={
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_EVENT,ssc)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(
      record => {
        val eventlog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        val dateHourStr: String = sdf.format(new Date(eventlog.ts))
        val dateArr: Array[String] = dateHourStr.split(" ")
        eventlog.logDate = dateArr(0)
        eventlog.logHour = dateArr(1)
        (eventlog.mid, eventlog)
      }
    )
    val midToLogByWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = midToLogByWindowDStream.groupByKey()

    val boolToAlertInfoDStream: DStream[(Boolean,CouponAlertInfo)] = midToLogIterDStream.map { case (mid, iter) =>

      val uids: util.HashSet[String] = new util.HashSet[String]()

      val itemIds = new util.HashSet[String]()

      val events = new util.ArrayList[String]()

      var noClick: Boolean = true

      breakable {
        iter.foreach(f = log => {

          val evid: String = log.evid

          events.add(evid)

          if ("coupon".equals(evid)) {
            uids.add(log.uid)
            itemIds.add(log.itemid)
          } else if ("clickItem".equals(evid)) {
            noClick = false
            break()

          }
        })

      }

      (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }

    val alterInfoDStream: DStream[CouponAlertInfo] = boolToAlertInfoDStream.filter(_._1).map(_._2)

    alterInfoDStream.cache()
    alterInfoDStream.print()

    val todayStr: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
    val indexName = s"${GmallConstant.ES_ALERT_INDEX_PRE}-$todayStr"

    alterInfoDStream.foreachRDD(rdd=>{
        rdd.foreachPartition(iter=>{
          val todayStr:String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)

          val docList: List[(String, CouponAlertInfo)] = iter.toList.map(alterInfo => {
            val min: Long = alterInfo.ts / 1000 / 60
            (s"${alterInfo.mid}-$min", alterInfo)

          })

          MyEsUtil.insertBulk(indexName,docList)

        })
    })
          ssc.start()
          ssc.awaitTermination()

  }
}
