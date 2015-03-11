package examples

import com.couchbase.client.java.document.JsonDocument
import org.apache.spark.{SparkContext, SparkConf}
import com.couchbase.spark._

object ReadById {

  def main(args: Array[String]): Unit = {
    // Create a spark config
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("readById")
      .set("com.couchbase.bucket.default", "")

    // Create the spark context
    val sc = new SparkContext(conf)

    // Fetch documents by IDs
    sc.couchbaseGet[JsonDocument](Seq("pymc0", "pymc1"))
      .map(doc => doc.content().getString("name") + " - " + doc.content().getInt("age"))
      .collect()
      .foreach(println)
  }

}
