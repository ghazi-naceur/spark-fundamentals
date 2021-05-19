name := "spark-fundamentals"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-hive" % "3.0.0",
  "org.apache.spark" %% "spark-avro" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.0",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % "3.0.0",
//"org.scala-lang" % "scala-library" % "2.12.0",
"org.apache.hadoop" % "hadoop-streaming" % "2.7.1",
"org.scalafx" % "scalafx_2.12" % "8.0.102-R11",
"org.apache.spark" %% "spark-sql" % "3.0.0",
"org.apache.spark" %% "spark-graphx" % "3.0.0",
"org.twitter4j" % "twitter4j-core" % "4.0.4",
"org.twitter4j" % "twitter4j-stream" % "4.0.4"
)
//"edu.trinity" %% "swiftvis2" % "0.1.0-SNAPSHOT"
//"edu.trinity" %% "swiftvis2spark" % "0.1.0-SNAPSHOT"
//assemblyJarName in assembly := s"${name.value.replace(' ', '-')}-${version.value}.jar"
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)