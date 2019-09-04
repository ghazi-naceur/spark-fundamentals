name := "spark-fundamentals"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-hive_2.12" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-avro_2.12" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-streaming-kinesis-asl_2.12" % "2.4.3"
//libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-streaming" % "2.7.1"
libraryDependencies += "org.scalafx" % "scalafx_2.12" % "8.0.102-R11"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.12" % "2.4.3"
//libraryDependencies += "edu.trinity" %% "swiftvis2" % "0.1.0-SNAPSHOT"
//libraryDependencies += "edu.trinity" %% "swiftvis2spark" % "0.1.0-SNAPSHOT"
//assemblyJarName in assembly := s"${name.value.replace(' ', '-')}-${version.value}.jar"
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)