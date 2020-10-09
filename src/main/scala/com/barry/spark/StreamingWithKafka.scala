package com.barry.spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * author borui.shao@earlydata.com
 * Date 2020/9/17 5:13 PM
 * VERSION 1.0
 * Describe TODO
 **/

object StreamingWithKafka {

  private val brokerList = "192.168.3.136:9092,192.168.3.136:9093,192.168.3.136:9094"
  private val topic = "topic-spark"
  private val group = "group-spark"
  private val checkpointDir = "/opt/data/kafka/checkpoint"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWithKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointDir)

    val kafkaParam = Map[String, Object](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParam))

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(iter => {
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId())
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      })
    })

    //单独处理最近2秒钟的数据
    val value = stream.map(record => {
      val intVal = Integer.valueOf(record.value())
      println(intVal + " == " + record.offset())
      intVal
    }).reduce(_ + _)

    value.print()

    //窗口大小10s，每个窗口间隔2秒 {--[--------}--]
    val winValue = stream.map(record => {
      val intVal = Integer.valueOf(record.value())
      println(intVal + " === " + record.offset())
      intVal
    }).reduceByWindow(_ + _, Seconds(20), Seconds(2))

    winValue.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
