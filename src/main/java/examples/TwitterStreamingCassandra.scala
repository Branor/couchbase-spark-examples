//package examples
//
//import com.datastax.spark.connector.SomeColumns
//import com.datastax.spark.connector.cql.CassandraConnector
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.twitter._
//import com.datastax.spark.connector.streaming._
//
//object TwitterStreamingCassandra {
//
//  def main(args: Array[String]): Unit = {
//
//    val window = 5
//    //val filter = Seq("couchbase", "nosql", "kafka", "storm", "spark")
//    //val filter = Seq("ndc", "oslo", "norway")
//    val filter = Seq("usa", "iran", "russia");
//
//    val conf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("twitterStreaming")
//      .set("spark.cassandra.connection.host", "localhost")
//
//    CassandraConnector(conf).withSessionDo { session =>
//      session.execute(s"CREATE KEYSPACE IF NOT EXISTS twitter WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
//      session.execute(s"CREATE TABLE IF NOT EXISTS twitter.tweets (id bigint PRIMARY KEY, user text, text text, date timestamp, lat double, lon double)")
//      session.execute(s"TRUNCATE twitter.tweets")
//    }
//
//    val ssc = new StreamingContext(conf, Seconds(window))
//
//
//    val tweets = TwitterUtils
//      .createStream(ssc, None, filter)                                        // create the tweet stream
//      .map(mapFunc = tweet => {
//      // map tweets into a JsonDocument, keeping the username, text, date and location fields
//      val id = tweet.getId.toString
//      val user = tweet.getUser.getScreenName
//      val text = tweet.getText
//      val date = tweet.getCreatedAt.getTime
//      val location = tweet.getGeoLocation
//      val lat = if(location == null) 0 else location.getLatitude
//      val lon = if(location == null) 0 else location.getLongitude
//
//      val fields = (id.toLong, user, text, date, lat, lon)
//      println(fields)
//      fields
//    })
//    //  .print()
//    //.saveToCassandra("twitter", "tweets", SomeColumns("id", "user", "text", "date", "lat", "lon"))
//
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
