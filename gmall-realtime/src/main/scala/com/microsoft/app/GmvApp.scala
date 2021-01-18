package com.microsoft.app

import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.microsoft.bean.OrderInfo
import com.microsoft.constants.GmallConstant
import com.microsoft.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jenny.D
 * @create 2021-01-11 17:36
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO,ssc)

    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      val create_time: String = orderInfo.create_time
      val dateTimeArr: Array[String] = create_time.split(" ")

      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)

      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*********"

      orderInfo

    })
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
