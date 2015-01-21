package bigdata

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by king on 12/29/14.
 */
object KafkaMsgConsumer {
  def main (args: Array[String]) {

    val zkQ = "10.214.20.118"

//    if (args.length < 4) {
//      System.err.println("Usage: MsgConsumer <zkQuorum> <group> <topics> <numThreads>")
//      System.exit(1)
//    }
//
//    val Array(zkQuorum, group, topics, numThreads) = args
//
    val sparkConf = new SparkConf().setAppName("MsgConsumer").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//    ssc.checkpoint("checkpoint")
//
//    val topicpMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)
//

//    val mp = MsgManager(zkQ, sqlContext)
//    mp.processMsg()


    println("hello scala")
    sc.stop()

  }

}
