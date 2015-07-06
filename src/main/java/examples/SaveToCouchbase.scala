package examples

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import org.apache.spark.{SparkContext, SparkConf}
import com.couchbase.spark._

object SaveToCouchbase {

  def main(args: Array[String]): Unit = {

    // Create a spark config
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("readById")

    // Create the spark context
    val sc = new SparkContext(conf)

    // Create 100 documents and store them
    sc.parallelize(0 until 100)
      .map(i => JsonDocument.create("doc-" + i, JsonObject.create().put("number", i)))
      .saveToCouchbase()

    // Check that some of the documents exist
    sc.parallelize(Seq("doc-2", "doc-99"))
      .couchbaseGet[JsonDocument]()
      .collect()
      .foreach(println)
  }
}
