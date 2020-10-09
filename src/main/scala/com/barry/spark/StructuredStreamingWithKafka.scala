package com.barry.spark

import kafka.server.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


/**
 * author borui.shao@earlydata.com
 * Date 2020/9/17 7:52 PM
 * VERSION 1.0
 * Describe TODO
 **/

object StructuredStreamingWithKafka {

  private val brokerList = "192.168.3.136:9092,192.168.3.136:9093,192.168.3.136:9094"
  private val topic = "topic-spark"
  private val group = "group-spark"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("StructuredStreamingWithKafka").getOrCreate()
    import spark.implicits._

    // readStream 以流的方式读取数据
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("subscribe", topic)
      .load()

    val ds = df.selectExpr("Cast(value as string)").as[String]

    val words = ds.flatMap(_.split(" ")).groupBy("value").count()

    val query = words.writeStream.outputMode("complete")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .start()

    println(query.status)

    query.awaitTermination()

/****************************************************************/
    // read 以batch的方式读数据
//    val df = spark.read.format("kafka")
//      .option("kafka.bootstrap.servers", brokerList)
//      .option("subscribe", topic)
//      .option("startingOffsets",s"""{"$topic":{"0":23,"1":-2,"2":-2}}""")
//      .option("endingOffsets",s"""{"$topic":{"0":50,"1":-1,"2":-1}}""")
//      .load()
//
//    val ds = df.selectExpr("cast(key as string)","cast(value as string)")
//      .as[(String,String)]
//
//    ds.show()

  }

}
