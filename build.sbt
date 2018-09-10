name := "SparkExamples"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.0"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.1"
