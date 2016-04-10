package examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import com.couchbase.spark.streaming._

object ReadDcpStream {

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
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
