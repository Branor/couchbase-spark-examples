package examples

import com.couchbase.client.java.document.{JsonDocument, JsonArrayDocument}
import com.couchbase.client.java.document.json.{JsonObject, JsonArray}
import com.couchbase.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

object TwitterStreaming {

  def main(args: Array[String]): Unit = {

    val window = 5
    //val filter = Seq("couchbase", "nosql", "kafka", "storm", "spark")
    val filter = Seq("israel", "iran", "elections")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("twitterStreaming")
      .set("com.couchbase.nodes", "localhost")
      .set("com.couchbase.bucket.default", "")
      .set("com.couchbase.bucket.tweets", "")

    val ssc = new StreamingContext(conf, Seconds(window))

    val stream = TwitterUtils
      .createStream(ssc, None, filter) // create the stream
      .flatMap(status => status.getText.split(" ").filter(_.startsWith("#"))) // extract hashtags from tweets
      .map((_, 1)) // add 1 to each tag to prepare it for reduce
      .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(window), Seconds(window)) // reduce by a 1 second window and emit a new rdd in one second as well
      .map {case (topic, count) => (count, topic)} // flip the data for sorting
      .transform(_.sortByKey(false)) // sort descending
      .filter(_._1 >= 2) // filter out not so popular tags (for example set to 3)
      .map(countAndTopic => ("aggr", countAndTopic)) // map the count and topic onto a single key for easy grouping
      .groupByKey() // group all data into a single RDD item so we can store it as a document
      .map(data => { // map from the list of tuples into a JsonArrayDocument. use a custom document id per second
        val id = "_tags::" + System.currentTimeMillis() / 1000
        val content = JsonArray.create()
        data._2.foreach(tuple => content.add(JsonObject.create().put("tag", tuple._2).put("count", tuple._1)))
        println(id, content)
        JsonArrayDocument.create(id, content)
      })
      //.print()
      .saveToCouchbase("default") // store the document in couchbase


    val tweets = TwitterUtils
      .createStream(ssc, None, filter)
      .map(tweet => {
        val id = tweet.getId.toString
        val content = JsonObject.create()
        val location = tweet.getGeoLocation
        content.put("text", tweet.getText)
        content.put("date", tweet.getCreatedAt.getTime)
        content.put("user", tweet.getUser.getScreenName)

        if (location != null)
           content.put("location", JsonArray.create().add(location.getLongitude).add(location.getLatitude))
        println(id, content)
        JsonDocument.create(id, content)
      })
      //.print()
      .saveToCouchbase("tweets")


    ssc.start()
    ssc.awaitTermination()
  }
}
