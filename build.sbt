name := "epconsumer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0",
  "org.apache.spark" % "spark-sql_2.10" % "1.2.0",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.4.4",
  "org.scalatest" % "scalatest_2.10" % "2.2.3" % "test"
)
