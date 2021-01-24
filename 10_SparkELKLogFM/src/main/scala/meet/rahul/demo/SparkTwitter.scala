package meet.rahul.demo

import meet.rahul.demo.Utilities.setupLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level

object SparkTwitter extends Serializable {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir",System.getProperty("user.dir")+"\\hadoop\\")



    val sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "10.0.0.74").set("es.port", "9200").set("es.net.http.auth.user","")
      .set("es.net.http.auth.pass", "").set("es.nodes.wan.only", "false").set("es.nodes.discovery", "false")
    //val spark = SparkSession.builder().config(sparkConf).appName("sourcedashboard").master("local[*]").getOrCreate()

    // Configure Twitter credentials using twitter.txt
    Utilities.setupTwitter(System.getProperty("user.dir")+"\\twitter.txt")

    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())
    statuses.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
