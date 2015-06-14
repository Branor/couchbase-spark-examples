name := "couchbase-spark-examples"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.2.2" withSources() withJavadoc(),
  "com.couchbase.client" %% "spark-connector" % "1.0.0-dp",
  "org.apache.spark" %% "spark-streaming" % "1.2.1",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1"
)

resolvers += "Couchbase Repository" at "http://files.couchbase.com/maven2/"