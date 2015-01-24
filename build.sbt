name := "epconsumer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.2.0" % "provided",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.3.1"
)
