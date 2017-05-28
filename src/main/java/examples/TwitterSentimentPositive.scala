package examples

import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterSentimentPositive {

  def main(args: Array[String]): Unit = {

    System.setProperty("com.couchbase.dcpEnabled", "true")

    // Create a spark config
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("readById")
      .set("com.couchbase.bucket.tweets", "")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc
      .couchbaseStream()
      .filter(_.isInstanceOf[Mutation])
      .map(msg => new String(msg.asInstanceOf[Mutation].content, "UTF-8"))
      .map(JsonObject.fromJson(_))
      .filter(json => json.containsKey("sentimentScore") && json.getInt("sentimentScore") >= 3)
      .map(json => json.getString("sentiment") + " : " + json.getString("text"))
     // .count()
      .print()


    ssc.start()
    ssc.awaitTermination()
  }
}
