package cn.zju.edu.vlis.msgstore

import java.sql.Connection
import java.util.Properties

import oracle.jdbc.driver.OracleDriver

/**
 * Created by king on 1/15/15.
 */
class MsgOracleHelper(connpro: Properties) {

  val conn: Option[Connection] = {
//    val connpro = new Properties()
//    if (System.getProperty("dbconfig") == null) {
//      connpro.load(getClass.getResourceAsStream("/config.properties"))
//      println("/config.properties")
//    }
//    else {
//      println(System.getProperty("dbconfig"))
//      connpro.load(getClass.(System.getProperty("dbconfig")))
//    }
    val oracle = new OracleDriver()
    try {
      Some(oracle.connect(connpro.getProperty("url"), connpro))
    } catch {
      case e: Exception =>
        sys.exit(1)
    }
  }

  val stm = conn.get.createStatement()

  // 需要注意 ResultSet close
//  def executeQuery(sql: String): Option[ResultSet] = {
//    try {
//      Some(stm.executeQuery(sql))
//    } catch {
//      case e: Exception => None
//    }
//  }

  def executeUpdate(sql: String): Unit = {
    try {
      stm.executeUpdate(sql)
    } catch {
      case e: Exception =>
        println("update failed")
        e.printStackTrace()
    }
  }


}

object MsgOracleHelper {

  var helper: Option[MsgOracleHelper] = None

  def apply(connpro: Properties): MsgOracleHelper = {
    helper match {
      case Some(h) => h
      case None =>
        helper = Some(new MsgOracleHelper(connpro))
        helper.get
    }
  }
}