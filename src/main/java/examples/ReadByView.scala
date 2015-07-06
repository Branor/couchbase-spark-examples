package examples

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.view.ViewQuery
import org.apache.spark.{SparkContext, SparkConf}
import com.couchbase.spark._
import org.apache.spark.SparkContext._

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
    val beers = sc.couchbaseView(ViewQuery.from("beer", "brewery_beers"))
      .map(_.id)
      .couchbaseGet[JsonDocument]()
      .filter(doc => doc.content().getString("type") == "beer")
      .cache()

    // Calculate the mean for all beers
    println(beers
      .map(doc => doc.content().getDouble("abv").asInstanceOf[Double])
      .mean())

    // Find the top beers with the longest name
    beers
      .map(doc => doc.content().getString("name"))
      .map(name => (name.length, name))
      .sortBy(t => t._1, false)
      .take(3)
      .foreach(beer => println(beer._2 + ": " + beer._1))

  }

}
