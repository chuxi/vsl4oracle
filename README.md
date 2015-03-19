# vsl4oracle
Spark Streaming Kafka for Oracle Database (SQL) demo example


Recently I need to reconstruct one spark project which is used for processing messages received from Kafka. However, the original project just used spark streaming to accept, not really dealing with messages by streaming functions. And the structure of processing messages is quite disgusting, which deals with a message by complicated configuration file written by json format. At last, the original project uses hbase to store data. It leads to some very terrible situation...

Now I changed the project by merging spark SQL and replacing hbase by oracle. Oracle is required by the client and I want to improve the efficiency of message processing by using spark sql. So the key point of this new project is SQL. I can use SQL in configuration files, which reduced the complexity.

I planned new project as two parts. One is Kafka Message Parsing. Another is Storage. Two parts are connected by RDD. I processed message and exacted necessary data by spark sql and get a RDD\[Row\]. Then store it into oracle database. From the view of spark, it is an easy process. However, what kind of messages are you going to deal with determines many things.

(why I wrote these by English? Because I am working in the ZJU lab and do not want to leave any guides here when I graduate one year later. The 'project manager' will tell other company clients that we have a complete system to process any kind of messages by spark streaming and sql framework. It is not honest, right? I am the only one who is doing this work and it is far from a complete system. I am always asked to write some fake documents without coding work. It is some fake work and fucking fake life. )

I would like to introduce my project from spark streaming, how to adapt spark sql with streaming, configuration files and storage. The source code is in my github [vsl4oracle](https://github.com/chuxi/vsl4oracle) project.

### Spark Streaming

If you want to use spark streaming, you must know your data is coming on a waterline. I just used a small part of whole functions, no Windows, Slides. I designed my project based on RDDs, which is called DStream.

DStream, It has been detailed introduced on the spark streaming website, a wrapper of many RDDs. It likes a waterline, which contains real-time streams. And it is a common interface for others to connect, such as Kafka, Flume, etc. DStream receives data from interfaces and processes data by a RDD unit. Spark Streaming provided a lot of functions to "Map and Reduce" RDDs, which is in a DStream. The concept of DStream is a key point for streaming programmers to understand spark streaming.

I started my KafkaMsgConsumer.scala from some parameters setting. I need zookeeper cluster info for kafka connection, kafka groups, topics and threads, and oracle connection information. At last, I also set the log level by the following codes(It is found by my colleague who is reading spark source code, not introduced in the programming guide).

    val pro = new Properties()
    pro.put("log4j.rootLogger", "ERROR, console")
    PropertyConfigurator.configure(pro)

Then you must set spark streaming and spark sql under the same spark context, because one JVM only could contain one spark context.

    val sparkConf = new SparkConf().setAppName("MsgConsumer")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(2))

Following we start receiving data(RDDs) from kafka interface.

    val topicpMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)

There are some changes after I got messages from kafka. I want to group messages by some chars in it. DStream allows you do transformations and requires one output operation on it. You must keep the data in RDD format if you want to continue processing data by spark sql, which leads you can not use groupby to reconstruct RDD.

Finally I choose to filter the DStream several times to "cut" the DStream into several parts.

    lines.foreachRDD { rdd =>
        // operate FTVD7D message
        process(MsgFTVD7D(MsgOracleHelper(connpro)), rdd.filter(s => s.substring(3, 9).equals("FTVD7D")))
        // operate VDL24H message
        process(MsgVDL24H(MsgOracleHelper(connpro)), rdd.filter(s => s.substring(3, 9).equals("VDL24H")))
    }

I did not find any other solution in spark community. And I think spark did not offer the I-Wanted groupby method is correct. RDD is already divided into partitions. So I am sure there is no other choice. Because I have to deal with a batch of messages by its type.

I defined the process method in main method which is introduced in Configuration and Spark SQL part. The code is not perfect. I know I could adjust the structure to make it better performance( Now I am in lazy val state...)

---

### Configuration File (JSON)

I suffered a lot to design the message configuration file. First try to parse the message directly which code is very very terrible. Then I wrote out the first kind of configuration file which has no MOJO structure which means I could not use a Mapped-Class to parse the configuration. I used the JsonNode to read configurations. I implemented the code and it uses hbase to store data, which is very disgusting... Then I started to reconstruct the project again. When I almost completed it( I rewrote the hbase helper class and transformed it from Java to Scala implementation), my project manager told me to use oracle as database......All right, So I started to learn Oracle and finally implemented it again.

I groped some principles for making configuration files used for message.

+ You must realize you would process the whole message several times, so most outside, it should be an array (Json).
+ Everytime when you are processing the message, you should keep the elements selected in the same loop level.
+ Leave one factor to determine how to deal with the selected elements, if the message elements has some different processing ways.
+ Set a hooker in case for the factor in Step 3.
+ Configure the storage (or could check) method in a case.

Message is made up from records (or groups), record is made up from elements. One record could loop multiple times in a message. For example:

    00:FTVD7D:WW1322080811410#0000048201410100843:9:132208081:002403081b:201410100839:金云杰'
    10:W1322080811410#0000048:B9329588:德翔鹿特丹:WMS ROTTERDAM:14040W:1:1:HKHKG:中国香港::日本:20141013::0100169::N::9329588'
    30:0:0:0:0:0'
    30:0:0:0:0:0'
    40:150:::7464:3085:12464:德翔:德翔:999:8:19'
    99:5'

upside, it is a message in my case. It is plain message, which has no loops. I doubled the record `30` to make a loop.

So First we want to parse the message and store the whole message in database. It leads to operate the message at least two times. In my first project I want to parse the project one time including the loops, but failed in other kinds of message. You have to process loops one by one element and it caused serious problem. By first two steps design, I could use spark sql to select elements of batch messages.

Then step 3 \& 4 connected the extraction with storage. At last do storage in Step 5. Your storage could be any type, which would not influence outside structure.

Following is the demo message configuration file I designed for.

    [
      {
        "records" : ["00", "10"],
        "cells"   : [2, 13, 8, 9, 10, 11, 18, 19, 20],
        "factors" : [0, 1],
        "cases"   : [
          {
            "value" : 91,
            "sql"   : "insert into epvsl(vslid, vslnamecn, vslname, impvoy, berpln, port, flag, canceled) values('%s', '%s', '%s', '%s', '%s', '%s', %s, %s)",
            "vs"    : [
              2, 3, 4, 5, 6, 8, 1, 0
            ]
          },
          {
            "value" : 92,
            "sql"   : "update epvsl set expvoy='%s', unberpln='%s', flag=2 where vslname='%s' and flag=1",
            "vs"    : [
              5, 7, 4
            ]
          },
          {
            "value" : 31,
            "sql"   : "update epvsl set flag=2, canceled=3 where vslname='%s' and flag=1 and canceled=9",
            "vs"    : [
              4
            ]
          }
        ]
      },
      {
        "records" : ["10"],
        "cells"   : [3, 18],
        "factors" : [],
        "cases"   : [
          {
            "sql"   : "insert into epvslmsg(vslname, content) values('%s', '%s')",
            "vs"    : [
              0, 1
            ]
          }
        ]
      }
    ]

All the work I have done, aims for the JSON MOJO Mapper. So I do not need any JsonNode. I create two case class to map the configuration file json structure.

    // configuration file json Mapper
    case class MsgConfCase(var value: Int, var sql: String, var vs: Array[Int])
    case class MsgConf(var records: Array[String], var cells: Array[Int], var factors: Array[Int], var cases: Array[MsgConfCase])

And the configuration file loading could be very easy.

    val root = {
      val mp = new ObjectMapper
      mp.registerModule(DefaultScalaModule)
      mp.readValue(getClass.getResourceAsStream(f), classOf[Array[MsgConf]])
    }

---

### Spark SQL

It is up to the count of messages whether applying spark sql or not. To thousands of messages in a RDD, it is efficient and easy to apply spark sql. However, if there are only several, directly processing messages is better.

    def process(msgtype: MsgCommon, rdd: RDD[String]): Unit = {
      if (rdd.count() != 0) {
        msgtype.root.foreach { mc =>
          val tRDD = rdd.map(s => s.split("'").filter(m => mc.records.contains(m.take(2))).toList.map(_.split(":").toList.tail).flatten :+ s.replaceAll("'", "''")).map(p => Row(p: _*))

          // tRDD.foreach(println)
          // generate Schema
          val schema = StructType(Range(0, tRDD.first().length).map("q"+_).map(fieldName => StructField(fieldName, StringType, true)))
          // schema.printTreeString()

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

I process a message RDD as the configuration file. First, I extract the elements wanted in the RDD. Then make up a schema of the elements in the RDD. Following, I apply the schema to the RDD to make a schemaRDD. So now I can use spark sql to the schemaRDD. Finally, I just need to collect and then store the whole RDD result to database. Because my message is not a json or parquet file. It just contains the data and the protocol is defined outside. I have to choose the applySchema function to implement the spark sql functions. If your message data is different, I think the other method would be better, directly reading data into schemaRDD.

---

### Storage

I ever completed a HBase Version of storage. It is so complicated and I have no sql to deal with all situations. The main difficulty is how to locate the row key when you are updating or deleting. And there's no index in HBase, how to keep the performance becomes a big problem. It means you must design your HBase schema carefully and well know attributes of HBase. ( Of course, now I already know these well ^_^ )

But, Here I just want to talk about the convenience of SQL when you are storing data. There is no doubt that you can directly insert rows into database, including RDBMS and NoSQL. When you come to update or delete, it would be very difficult. You need to set up conditions in your configuration file to locate the rows which would be changed. If you want to set foreign keys, it requires the Database you are using has the attributes to support it. To HBase, I set a column family to store foreign keys and make the qualifiers in the column family equal to the foreign keys.

It would be quite different to design a schema in NoSQL. Apparently RDBMS are much better in implementation and performance. However, it is a distributed system, you can not avoid the large quantity of data, which RDBMS can not deal with.

---

I complete this document to record the work I did and offer some tips to others. I believe this demo example could be developed into a large one. I have processed two kinds of message and my test shows it works really well. If someone want to know how to processing messages by spark streaming with Kafka and combine spark sql, I think it would be a good example. That's all.

