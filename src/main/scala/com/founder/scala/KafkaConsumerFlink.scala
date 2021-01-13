package com.founder.scala

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * 通过消费kafka的数据，
 * 1，存在mysql中
 * 2，存在hbase里面
 *
 */
object KafkaConsumerFlink {


  def main(args: Array[String]): Unit = {
    val kafkaProps = new Properties()
    //kafka的一些属性
    kafkaProps.setProperty("bootstrap.servers", "10.10.5.36:9092")
    //所在的消费组
    kafkaProps.setProperty("group.id", "group_test")
    //获取当前的执行环境
    val evn = StreamExecutionEnvironment.getExecutionEnvironment
    //kafka的consumer，test1是要消费的topic
    val kafkaSource = new FlinkKafkaConsumer[String]("flinkTest4", new SimpleStringSchema, kafkaProps)
    //设置从最新的offset开始消费
    kafkaSource.setStartFromLatest()
    //自动提交offset
    kafkaSource.setCommitOffsetsOnCheckpoints(true)

    //flink的checkpoint的时间间隔
    evn.enableCheckpointing(5000)

    //添加consumer
    val stream = evn.addSource(kafkaSource)

    val mysqlSink = new MysqlSink("jdbc:mysql://127.0.0.1:3306/flink?characterEncoding=utf8&useSSL=true","root","123456")
    stream.addSink(mysqlSink)


    val oracleSink = new OracleSink("jdbc:oracle:thin:@10.10.6.6:1521/orcl","nhipadmin","nhipFounder")
    stream.addSink(oracleSink)

// HBaseSink 消费到HBase中
    val hbaseSink = new HBaseSink("student","f")
    stream.addSink(hbaseSink).name("hbaseSink")
    //    text.print()
    //启动执行
    evn.execute("kafkawd")


  }
}
