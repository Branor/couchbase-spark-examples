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
      .set("com.couchbase.bucket.default", "")
      .set("com.couchbase.nodes", "localhost")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc
      .couchbaseStream(from = FromBeginning, to = ToInfinity)
      .filter(!_.isInstanceOf[Snapshot])
      .count()
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
