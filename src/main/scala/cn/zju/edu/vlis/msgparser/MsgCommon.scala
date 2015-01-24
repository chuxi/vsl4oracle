package cn.zju.edu.vlis.msgparser

import cn.zju.edu.vlis.msgstore.MsgOracleHelper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StringType, StructField, StructType}

/**
 * Created by king on 1/5/15.
 */

// 定义配置文件的json格式 Mapper
case class MsgConfCase(var value: Int, var sql: String, var vs: Array[Int])
case class MsgConf(var records: Array[String], var cells: Array[Int], var factors: Array[Int], var cases: Array[MsgConfCase])

// 一种报文定义一个case class
case class MsgFTVD7D(h: MsgOracleHelper, f: String = "/msgconfig/cFTVD7D.json") extends MsgCommon(h, f)
case class MsgVDL24H(h: MsgOracleHelper, f: String = "/msgconfig/cVDL24H.json") extends MsgCommon(h, f)


class MsgCommon(helper: MsgOracleHelper, f: String) extends MsgTrait{
  val root = {
    val mp = new ObjectMapper
    mp.registerModule(DefaultScalaModule)
    mp.readValue(getClass.getResourceAsStream(f), classOf[Array[MsgConf]])
  }

  val ca = root.filter(!_.factors.isEmpty).map(_.cases).flatten.groupBy(_.value).toMap

  val tbname: String = "eptb"

  /**
   * parse a message RDD, extract the required data
   * @param msgs an String RDD, register the extracted data as a table
   */
  override def parse(mc: MsgConf, msgs: List[String], sqlContext: SQLContext): Unit = {
    val t = msgs.map(s => s.split("'").filter(m => mc.records.contains(m.take(2))).toList.map(_.split(":").toList.tail).flatten :+ s.replaceAll("'", "''"))

//    t.foreach(println)
    // 生成 Schema
    val schema = StructType(Range(0, t(0).length).map("q"+_).map(fieldName => StructField(fieldName, StringType, true)))
//    schema.printTreeString()
    // 生成 RDD Rows

    println(sqlContext.sparkContext)

    val tRDD = sqlContext.sparkContext.parallelize(t).map(p => Row(p: _*))

    tRDD.collect().foreach(println)

    val dataRDD = sqlContext.applySchema(tRDD, schema)

    dataRDD.registerTempTable(tbname)

    Some(sqlContext.sql("select " + mc.cells.map("q"+_).mkString(",") + s" from $tbname").collect())


//      result.collect().foreach(println)

  }

  /**
   * based on the config json file, store data
   */
  override def store(rdd: Option[Array[Row]], mc: MsgConf): Unit = {
    rdd.get.foreach{ r =>
      val c = mc.factors.isEmpty match {
        case true =>
          mc.cases
        case false =>
//          println(mc.factors.map(r(_)).mkString.toInt)
          ca.get(mc.factors.map(r(_)).mkString.toInt).get
      }

      for (e <- c) {
        println(e.sql.format(e.vs.map(r(_)): _*))
        helper.executeUpdate(e.sql.format(e.vs.map(r(_)): _*))
      }

    }
  }

  /**
   * validate and transform the message data in RDD table
   */
  override def check: Unit = {

  }
}
