package com.founder.scala

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import scala.concurrent._

/**
 * Created by nick on 19.6.5
 * 通过flink消费kafka的数据，然后显示在控制台上
 *
 */
object KafkaConsumer2Flink {
  def main(args: Array[String]): Unit = {
    val kafkaProps = new Properties()
    //kafka的一些属性
    kafkaProps.setProperty("bootstrap.servers", "10.10.5.36:9093")
    //所在的消费组
    kafkaProps.setProperty("group.id", "group_test")
    //获取当前的执行环境
    val evn = StreamExecutionEnvironment.getExecutionEnvironment
    //kafka的consumer，test1是要消费的topic
    val kafkaSource = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema, kafkaProps)
    //设置从最新的offset开始消费
    kafkaSource.setStartFromLatest()
    //自动提交offset
    kafkaSource.setCommitOffsetsOnCheckpoints(true)

    //flink的checkpoint的时间间隔
    evn.enableCheckpointing(5000)
    //添加consumer
    val stream = evn.addSource(kafkaSource)
    stream.setParallelism(3)
    val text = stream


    text.print()
    //启动执行
    evn.execute("kafkawd")


  }
}
