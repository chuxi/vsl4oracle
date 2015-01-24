package bigdata


import java.util.Properties

import cn.zju.edu.vlis.msgparser.{MsgVDL24H, MsgCommon, MsgFTVD7D}
import cn.zju.edu.vlis.msgstore.MsgOracleHelper
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by king on 12/29/14.
 */


object KafkaMsgConsumer {
  def main (args: Array[String]) {
    if (args.length < 7) {
      System.err.println("Usage: MsgConsumer <zkQuorum> <group> <topics> <numThreads> <database_url> <username> <password>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, url, user, password) = args
    val connpro = new Properties()
    connpro.setProperty("url", s"jdbc:oracle:thin:@$url:1521:orcl")
    connpro.setProperty("user", user)
    connpro.setProperty("password", password)

    val tbname: String = "eptb"

    val pro = new Properties()
    pro.put("log4j.rootLogger", "ERROR, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    PropertyConfigurator.configure(pro)


    val sparkConf = new SparkConf().setAppName("MsgConsumer")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicpMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)

    def process(msgtype: MsgCommon, rdd: RDD[String]): Unit = {
      if (rdd.count() != 0) {
        msgtype.root.foreach { mc =>
          val tRDD = rdd.map(s => s.split("'").filter(m => mc.records.contains(m.take(2))).toList.map(_.split(":").toList.tail).flatten :+ s.replaceAll("'", "''")).map(p => Row(p: _*))

          //                  tRDD.foreach(println)
          // 生成 Schema
          val schema = StructType(Range(0, tRDD.first().length).map("q"+_).map(fieldName => StructField(fieldName, StringType, true)))
          //        schema.printTreeString()

          val dataRDD = sqlContext.applySchema(tRDD, schema)

          dataRDD.registerTempTable(tbname)

          val rs = Some(sqlContext.sql("select " + mc.cells.map("q"+_).mkString(",") + s" from $tbname").collect())

          rs.get.foreach(println)
          if (rs.isDefined) {
            msgtype.store(rs, mc)
          }
        }
      }
    }

    lines.foreachRDD { rdd =>
        // 操作 FTVD7D 类型报文
        process(MsgFTVD7D(MsgOracleHelper(connpro)), rdd.filter(s => s.substring(3, 9).equals("FTVD7D")))
        // 操作 VDL24H 类型报文
        process(MsgVDL24H(MsgOracleHelper(connpro)), rdd.filter(s => s.substring(3, 9).equals("VDL24H")))
    }

    println("hello scala")
    ssc.start()
    ssc.awaitTermination()

  }

}
