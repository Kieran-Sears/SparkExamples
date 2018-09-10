package TwitterLiveStreaming

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utils.Utilities._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object PrintTweets extends App {

  var totalTweets = new AtomicLong(0)
  var totalChars = new AtomicLong(0)

  setupTwitter()
  setupLogging()

  val conf = new SparkConf()
  conf setMaster "local[*]" // TODO only for local dev, comment out for deployment
  conf setAppName "PrintTweets"

  val streamingServiceContext = new StreamingContext(conf, Seconds(1))


  val stream = TwitterUtils.createStream(streamingServiceContext, None)
  val tweets = stream.map(status => status.getText)
  val lengths = tweets.map(tweet => tweet.length)

  saveToFile()
  meanCharactersPerTweet()

  streamingServiceContext.checkpoint("SomeCheckPoint")
  streamingServiceContext.start()
  streamingServiceContext.awaitTermination()

  def saveToFile() {
    tweets.foreachRDD((x, y) =>
      (x, y) match {
        case (rdd: RDD[String], time: Time) if rdd.count > 0 =>
          rdd
            .repartition(1)
            .cache()
            .saveAsTextFile("tweets_" + time.milliseconds.toString)
    })
  }

  def meanCharactersPerTweet() = {
    lengths.foreachRDD(x =>
      x match {
        case rdd: RDD[Int] if rdd.count > 0 =>
          totalTweets.getAndAdd(rdd.count)
          totalChars.getAndAdd(rdd.reduce((x, y) => x + y))
    })
  }

  def popularHashTags() ={
    tweets.foreachRDD(x => {
      x.map(tweet => tweet.split(" ").filter(_.startsWith("#")))
    })
  }

}
