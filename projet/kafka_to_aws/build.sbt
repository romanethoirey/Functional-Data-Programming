name := "kafka-to-aws"

version := "0.1"


scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.0.1"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.0.1"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.8.0-alpha2"
libraryDependencies +=  "com.amazonaws" % "aws-java-sdk" % "1.3.32"
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"
