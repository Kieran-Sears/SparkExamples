name := "SparkExamples"

version := "0.1"

scalaVersion := "2.11.12"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
//libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.1"
//
//libraryDependencies ++= Seq(
//  "org.twitter4j" % "twitter4j-core" % "4.0.4",
//  "org.twitter4j" % "twitter4j-stream" % "4.0.4"
//)
//
//libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "0.9.0-incubating"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)