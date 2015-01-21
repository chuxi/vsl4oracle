package cn.zju.edu.vlis.msgstore

import java.sql.{ResultSet, Connection}
import java.util.Properties

import oracle.jdbc.driver.OracleDriver

/**
 * Created by king on 1/15/15.
 */
class MsgOracleHelper {

  val conn: Option[Connection] = {
    val connpro = new Properties()
    if (System.getProperty("dbconfig") == null) {
      connpro.load(getClass.getResourceAsStream("/config.properties"))
    }
    else {
      connpro.load(getClass.getResourceAsStream(System.getProperty("dbconfig")))
    }
    val oracle = new OracleDriver()
    try {
      Some(oracle.connect(connpro.getProperty("url"), connpro))
    } catch {
      case e: Exception => None
    }
  }

  val stm = conn.get.createStatement()

  // 需要注意 ResultSet close
  def executeQuery(sql: String): Option[ResultSet] = {
    try {
      Some(stm.executeQuery(sql))
    } catch {
      case e: Exception => None
    }
  }

  def executeUpdate(sql: String): Unit = {
    try {
      stm.executeUpdate(sql)
    } catch {
      case e: Exception => println("update failed")
    }
  }


}

object MsgOracleHelper {

  var helper: Option[MsgOracleHelper] = None

  def apply(): MsgOracleHelper = {
    helper match {
      case Some(h) => h
      case None =>
        helper = Some(new MsgOracleHelper())
        helper.get
    }



  }
}