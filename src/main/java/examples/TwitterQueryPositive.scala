package examples

import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark._
import com.couchbase.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object TwitterQueryPositive {

  def main(args: Array[String]): Unit = {

    System.setProperty("com.couchbase.queryEnabled", "true")

    // Create a spark config
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("getByQuery")
      .set("com.couchbase.nodes", "localhost")
      .set("com.couchbase.bucket.default", "123456")
      .set("com.couchbase.bucket.tweets", "123456")

    // Create the spark context
    val sc = new SparkContext(conf)

    // Spark SQL Setup
    val sql = new SQLContext(sc)


    val docs = sc
      .couchbaseQuery(N1qlQuery.simple("SELECT `user`, text, sentimentScore, sentiment FROM tweets WHERE sentimentScore >= 3 "), "default")
      .cache()

    docs
      .filter(row => row.value.getInt("sentimentScore") == 4)
      .map(row => row.value.getString("sentiment") + " - " + row.value.getString("text"))
      .foreach(println)

    // Calculate the mean age
    println("Mean sentiment score = " + docs
      .map(row => row.value.getLong("sentimentScore").asInstanceOf[Long])
      .mean())


//    // Create a DataFrame with Schema Inference
//    val df= sql.read.couchbase(schema = StructType(
//       StructField("user", StringType) ::
//       StructField("text", StringType) ::
//       StructField("sentimentScore", LongType) ::
//       StructField("sentiment", StringType) :: Nil
//    ))
//
//    df.printSchema()
//
//    // SparkSQL Integration
//    df
//      .select("sentiment", "sentimentScore", "user", "text")
//      .sort(df("sentimentScore").desc)
//      .show(10)
  }
}
