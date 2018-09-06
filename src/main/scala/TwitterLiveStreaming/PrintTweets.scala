package TwitterLiveStreaming

import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext
import Utilities._
import org.apache.spark.rdd.RDD

object PrintTweets extends App {

  // these side effect counters should be stored in a table
  var totalTweets = new AtomicLong(0)
  var totalChars = new AtomicLong(0)

  // initialise the receiver
  setupTwitter()
  setupLogging()

  val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))


  val stream = TwitterUtils.createStream(ssc, None)
  val tweets = stream.map(status => status.getText)
  val lengths = tweets.map(tweet => tweet.length)

  saveToFile()
  meanCharactersPerTweet()

  ssc.checkpoint("SomeCheckPoint")
  ssc.start()
  ssc.awaitTermination()

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
      x.map(tweet => tweet.split(" ").filter(_.startsWith("#"), 1))
    })
  }

}
