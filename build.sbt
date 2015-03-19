name := "vsl4oracle"

version := "1.0"

scalaVersion := "2.10.4"

//resolvers += "oschina" at "http://maven.oschina.net/content/groups/public/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.0" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.3.0" % "provided",
  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.4.4"
)
