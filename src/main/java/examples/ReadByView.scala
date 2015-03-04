package examples

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.view.ViewQuery
import org.apache.spark.{SparkContext, SparkConf}
import com.couchbase.spark._

object ReadByView {

  def main(args: Array[String]): Unit = {

    // Create a spark config
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("readById")
      .set("com.couchbase.bucket.beer-sample", "") // connect to beer-sample instead of default

    // Create the spark context
    val sc = new SparkContext(conf)

    // Read the first 10 rows and load their full documents
    sc.couchbaseView(ViewQuery.from("beer", "brewery_beers").limit(10))
      .map(_.id)
      .couchbaseGet[JsonDocument]()
      .collect()
      .foreach(println)
  }

}
