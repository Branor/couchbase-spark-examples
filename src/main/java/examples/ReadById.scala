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
      .set("com.couchbase.bucket.beer-sample", "") // connect to beer-sample instead of default

    // Create the spark context
    val sc = new SparkContext(conf)

    // Fetch beers by IDs, filter out the name and print it out
    sc.couchbaseGet[JsonDocument](Seq("21st_amendment_brewery_cafe-21a_ipa", "aass_brewery-genuine_pilsner"))
      .map(doc => doc.content().getString("name"))
      .collect()
      .foreach(println)
  }

}
