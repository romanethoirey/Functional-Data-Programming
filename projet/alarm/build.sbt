name := "csv-parser"

version := "0.1"


scalaVersion := "2.11.8"


libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.0.1"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.0"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.8.0-alpha2"

