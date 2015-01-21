package cn.zju.edu.vlis

import cn.zju.edu.vlis.msgparser.{MsgFTVD7D, MsgVDL24H}
import cn.zju.edu.vlis.msgstore.MsgOracleHelper
import org.apache.spark.sql.SQLContext

/**
 * Created by king on 12/29/14.
 * 建立 HBase 集群连接管理
 * 处理 messages生成的RDDs
 *    1. message分类，分别生成RDD
 *    2. 对一类 message 的RDD根据配置文件调用SQL进行提取
 *    3. 对提取出的数据进行验证
 *    4. 存储入HBase
 */
class MsgManager private (sqlContext: SQLContext) {
  val h = MsgOracleHelper()
  val ftvd7d = MsgFTVD7D(h)
  val vdl24h = MsgVDL24H(h)


  def processMsg(tp: String, ms: List[String]): Unit = {
    val p = tp match {
      case "FTVD7D" => ftvd7d
      case "VDL24H" => vdl24h
    }

//    p.parse(ms, sqlContext)
//    p.check
//    p.store
    p.process(ms, sqlContext)

  }

}

object MsgManager {

  var m: Option[MsgManager] = None

  def apply(sqlContext: SQLContext): MsgManager = {
    m match {
      case Some(manager) => manager
      case None =>
        m = Some(new MsgManager(sqlContext))
        m.get
    }
  }

}
