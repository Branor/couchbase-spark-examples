package examples

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.query.Query
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object ReadByQuery {

  def main(args: Array[String]): Unit = {

    System.setProperty("com.couchbase.queryEnabled", "true")

    // Create a spark config
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("getByQuery")
      .set("com.couchbase.nodes", "localhost")
      .set("com.couchbase.bucket.default", "")
      .set("com.couchbase.bucket.tweets", "")

    // Create the spark context
    val sc = new SparkContext(conf)

    val docs = sc
      .couchbaseQuery(Query.simple("SELECT name, age, body FROM `default` WHERE name IS NOT MISSING"), "default")
      .filter(row => row.value.getInt("age") < 50 )
      .cache()

    docs
      .map(row => row.value.getString("name") + " - " + row.value.getInt("age"))
      .foreach(println)

    // Calculate the mean age
    println(docs
      .map(row => row.value.getInt("age").asInstanceOf[Int])
      .mean())

    // Count frequency of non-trivial words, print top 10
    val tweets = sc
      .couchbaseQuery(Query.simple("select text FROM `tweets`"), "tweets")
      .flatMap(row => row.value.getString("text").split(" "))
      .filter(word => word.length >= 5)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(item => item.swap)
      .sortByKey(false)
      .map(item => item.swap)
      .take(10)
      .foreach(println)
  }
}
