package KafkaExamples

import Utils.Utilities._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


class KafkaWithSparkStreaming {

  val conf = new SparkConf()
  conf setMaster "local[*]" // TODO only for local dev, comment out for deployment
  conf setAppName "KafkaExample"

  val StreamSessionContext = new StreamingContext(conf, Seconds(1))

  setupLogging()

  val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
  val topics = List("testlogs").toSet
  val stream = KafkaUtils.createDirectStream[String, String](
    StreamSessionContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  val lines = stream.map(x => x.value())

  // something that needs replacing and is a horrible regex way of reading log info
  val requests = lines.map(x => {
    val matcher = apacheLogPattern().matcher(x)
    if (matcher.matches()) matcher.group(5)
  } )

  // Extract the URL from the request
  val urls = requests.map(x => {val arr = x.toString.split(" "); if (arr.size == 3) arr(1) else "[error]"})

  // Reduce by URL over a 5-minute window sliding every second
  val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

  // Sort and print the results
  val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
  sortedResults.print()


  StreamSessionContext.checkpoint(System.getProperty("user.dir") + "checkpoints/KafkaWithSparkStreaming/")
  StreamSessionContext.start()
  StreamSessionContext.awaitTermination()
}
