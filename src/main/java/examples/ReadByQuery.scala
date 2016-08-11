package examples

import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types.{DoubleType, StructField, StringType, StructType, IntegerType}
import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.spark.sql._

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

    // Spark SQL Setup
    val sql = new SQLContext(sc)


    val docs = sc
      .couchbaseQuery(N1qlQuery.simple("SELECT name, age FROM default WHERE name IS NOT MISSING AND age IS NOT MISSING"), "default")
      .filter(row => row.value.getInt("age") < 50 )
      .cache()

    docs
      .map(row => row.value.getString("name") + " - " + row.value.getInt("age"))
      .foreach(println)

    // Calculate the mean age
    println(docs
      .map(row => row.value.getInt("age").asInstanceOf[Int])
      .mean())


    // Create a DataFrame with Schema Inference
    //val df = sql.read.couchbase(schemaFilter = EqualTo("name", "pymc0"))
    val df= sql.read.couchbase(schema = StructType(
       StructField("name", StringType) ::
       StructField("age", IntegerType) ::
       StructField("body", StringType) :: Nil
    ))

    df.printSchema()

    // SparkSQL Integration
    df
      .select("name", "age")
      .sort(df("age").desc)
      .show(10)


//    // Count frequency of non-trivial words, print top 10
//    val tweets = sc
//      .couchbaseQuery(Query.simple("select text FROM `tweets`"), "tweets")
//      .flatMap(row => row.value.getString("text").split(" "))
//      .filter(word => word.length >= 5)
//      .map(word => (word, 1))
//      .reduceByKey(_ + _)
//      .map(item => item.swap)
//      .sortByKey(false)
//      .map(item => item.swap)
//      .take(10)
//      .foreach(println)
  }
}
