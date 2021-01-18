package com.microsoft.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author Jenny.D
 * @create 2021-01-06 23:10
 */
object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")

  val broker_list: String = properties.getProperty("kafka.broker.list")

  private val groupId: String = properties.getProperty("kafka.group.id")

   val kafkaParam = Map(
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],

    "group.id" -> groupId,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

    def getKafkaStream(topic:String,ssc:StreamingContext):InputDStream[ConsumerRecord[String,String]] = {

      val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,

        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
      )

      dStream

    }

}
