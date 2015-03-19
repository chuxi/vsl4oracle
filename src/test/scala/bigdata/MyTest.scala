package bigdata

import java.io.File
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import oracle.jdbc.driver.OracleDriver
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.io.Source

/**
 * Created by king on 12/30/14.
 */

case class Foo(var test: String, var myList: Array[Map[String, String]])
case class MsgConfCase(var value: Int, var sql: String, var vs: Array[Int])
case class MsgConf(var records: Array[String], var cells: Array[Int], var factors: Array[Int], var cases: Array[MsgConfCase])

class MyTest extends FunSuite with BeforeAndAfterAll {
  var sparkConf: SparkConf = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  var ssc: StreamingContext = null


  override def beforeAll() {
    sparkConf = new SparkConf().setAppName("MsgConsumer").setMaster("local[4]")
    sc = new SparkContext(sparkConf)
    sqlContext = new org.apache.spark.sql.SQLContext(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  /**
   * used for developing purpose by reading vsl msg files
   * @return a map, All messages are grouped by msg type
   */
  def getMsgList: List[String] = {
    val dir = new File(getClass.getResource("/vsl").getPath)
    val fs = if (dir.exists() && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }

    val getMsgsFromFile = (file: File) => {
      val fhandle = Source.fromFile(file, "gbk")
      val s = fhandle.getLines().toList.mkString
      fhandle.close()
      s
    }

    val set = Set("VDL24H", "FTVD7D")
    fs.map(getMsgsFromFile)
  }

  test("reading strings from msg files") {


    val msgs = getMsgList.groupBy(s => s.substring(3, 9)).filter(m => Set("VDL24H", "FTVD7D").contains(m._1)).toMap

//    val result = msgs.groupBy(s => s.substring(3, 9)).filter(m => set1.contains(m._1)).map(m => m._2.map(_.split("'").filter(m => set2.contains(m.take(2))).toList.map(_.split(":").toList.tail).flatten))

    msgs.map {
      case (mstype, ms) => {
        val t = ms.map { s =>
          s.split("'").filter(m => Set("00", "10").contains(m.take(2))).toList.map(_.split(":").toList.tail :+ s).flatten
        }

        // 生成 Schema
        val schema = StructType(Range(0, t(0).length).map("q"+_).map(fieldName => StructField(fieldName, StringType, true)))

        // 生成 RDD Rows
        val tRDD = sc.parallelize(t).map(p => Row(p: _*))
        tRDD.collect().foreach(println)

        val dataRDD = sqlContext.applySchema(tRDD, schema)

        dataRDD.registerTempTable("eptb")

        val port = "0190222"
        val result = sqlContext.sql(s"select * from eptb where q24=$port")


        result.collect().foreach(println)


      }
    }


  }

  test("read message json config file") {

    val mp = new ObjectMapper
    mp.registerModule(DefaultScalaModule)
//    val foo = mp.readValue("""{"test":"113123","myList":[{"test2":"321323"},{"test3":"11122"}]}""", classOf[Foo])


    val root = mp.readValue(getClass.getResourceAsStream("/msgconfig/cFTVD7D.json"), classOf[Array[MsgConf]])

    val cells = Array(0, 1, 2, 4, 6, 8, 9, 7, 3)

    for (c <- root) {
      c.factors.foreach(println)
      c.cases.foreach(s => println(s.sql))
    }

    print("hello scala")
  }

  // 无法使用插值构建sql语句
  test("String template test") {
    val cells = Array(0, 1, 2, 4, 6, 8, 9, 7, 3)
    val sql = "insert into epvsl(vslid, vslnamecn, vslname, impvoy, berpln, port, flag, canceled) values(${cells(2)}, $cells(3))"
    val ssql = StringContext(sql, "", "").s("hello", "baby")
    println(ssql)

    var sql1 = "insert into epvsl(vslid, vslnamecn, vslname, impvoy, berpln, port, flag, canceled) values(%s, %s, %s, %s)"

    println(sql1.format(cells.take(4): _*))

  }


  test("connect oracle database") {
    val connpro = new Properties()


    if (System.getProperty("dbconfig") == null) {
      connpro.load(getClass.getResourceAsStream("/config.properties"))
    }
    else {
      connpro.load(getClass.getResourceAsStream(System.getProperty("dbconfig")))
    }

    val oracle = new OracleDriver()

    val conn = oracle.connect(connpro.getProperty("url"), connpro)

    conn.createStatement().executeUpdate("insert into epvsl(vslid, vslnamecn, vslname, impvoy, berpln, port, flag, canceled) values('B9152234', '前进', 'PROPEL PROGRESS', 'PP02', '20141014', '0100142', 1, 9);")



    println(connpro.getProperty("url"))
    conn.close()
  }

  test("spark conf study") {
    sparkConf.getAll.foreach(println(_))
  }



}
