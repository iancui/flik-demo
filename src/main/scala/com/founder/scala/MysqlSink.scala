package com.founder.scala

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.founder.modules.flink.entity.Student
import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}


class MysqlSink(url: String, user: String, pwd: String) extends RichSinkFunction[String] {


  var conn: Connection = _

  var p: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, user, pwd)
    conn.setAutoCommit(false)

     p = conn.prepareStatement("replace into student(name,age,sex,sid) values(?,?,?,?)")
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {

    val g = new Gson()
    val s = g.fromJson(value, classOf[Student])

    println("invoke-mysql-" + value)

    try {
      p.setString(1, s.getName)
      p.setString(2, s.getAge.toString)
      p.setString(3, s.getSex)
      p.setString(4, s.getSid)
      p.execute()
      conn.commit()
    } catch {
      case e: Exception => println(e.getMessage)
    }

  }

  override def close(): Unit = {
    super.close()
    p.close()
    conn.close()

  }


}
