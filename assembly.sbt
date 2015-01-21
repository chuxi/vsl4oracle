assemblyJarName in assembly := "KafkaMsgConsumer.jar"

test in assembly := {}

mainClass in assembly := Some("bigdata/KafkaMsgConsumer.scala")

assemblyMergeStrategy in assembly := {
  case PathList("org", "scala-lang", ps @ _*)  => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName.contains("scala-")}
}