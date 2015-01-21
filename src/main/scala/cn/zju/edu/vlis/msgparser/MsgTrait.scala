package cn.zju.edu.vlis.msgparser

import org.apache.spark.sql.SQLContext

/**
 * Created by king on 1/5/15.
 */
trait MsgTrait {

  /**
   * parse a message RDD, extract the required data
   * @param msgs an String RDD, register the extracted data as a table
   */
  def parse(mc: MsgConf, msgs: List[String], sqlContext: SQLContext)

  /**
   * validate and transform the message data in RDD table
   */
  def check

  /**
   * based on the config json file, store data
   */
  def store(mc: MsgConf)

}
