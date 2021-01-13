package com.founder.scala

import com.founder.modules.flink.entity.Student
import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

/**
  * Created by nick on 19.6.14
  */

class HBaseSink(tableName: String, family: String) extends RichSinkFunction[String] {


  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "10.10.5.36:2181")
    conn = ConnectionFactory.createConnection(conf)
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val g = new Gson()
    val student = g.fromJson(value, classOf[Student])


    println("invoke-hbase-" + value)

    val t: Table = conn.getTable(TableName.valueOf(tableName))

    val put: Put = new Put(Bytes.toBytes(student.getSid))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(student.getName))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(student.getAge))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"), Bytes.toBytes(student.getSex))
    t.put(put)
    t.close()

  }

  override def close(): Unit = {
    super.close()
    conn.close()
  }
}
