package CassandraExamples

import Utils.Utilities._
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

class CassandraExample {

  val conf = new SparkConf()
  conf set ("spark.cassandra.connection.host", "127.0.0.1")
  conf setMaster "local[*]" // TODO only for local dev, comment out for deployment
  conf setAppName "CassandraExample"

  val streamingServiceContext = new StreamingContext(conf, Seconds(10))

  setupLogging()

  val lines = streamingServiceContext.socketTextStream(
    "127.0.0.1",
    9999,
    StorageLevel.MEMORY_AND_DISK_SER)

  val requests = lines.map(x => {
    val matcher = apacheLogPattern().matcher(x)
    if (matcher.matches()) {

      val ip = matcher.group(1)
      val request = matcher.group(5)
      val requestFields = request.toString.split(" ")
      val url = requestFields(1)
      (ip,url,matcher.group(6).toInt, matcher.group(9))
    } else ("error", "error", 0, "error")
  })

  requests.foreachRDD((rdd, time) => {
    rdd.cache()
    println("writing " + rdd.count() + " rows to cassandra")
    rdd.saveToCassandra("frank", "LogTest", SomeColumns("IP", "URL", "STATUS", "USER_AGENT"))
  })

  streamingServiceContext.checkpoint(System.getProperty("user.dir") + "checkpoints/CassandraExample/")
  streamingServiceContext.start()
  streamingServiceContext.awaitTermination()
}
