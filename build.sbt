//name := "couchbase-spark-examples"
//
//version := "1.0.0-SNAPSHOT"
//
//scalaVersion := "2.10.4"
//
//libraryDependencies ++= Seq(
//  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.2.2" withSources() withJavadoc(),
//  "org.apache.spark" %% "spark-core" % "1.4.0",
//  "org.apache.spark" %% "spark-streaming" % "1.4.0",
//  "org.apache.spark" %% "spark-sql" % "1.4.0",
//  "com.couchbase.client" %% "spark-connector" % "1.0.0-beta",
//  "org.apache.spark" %% "spark-streaming-twitter" % "1.4.0"
//)
//
//resolvers += "Couchbase Repository" at "http://files.couchbase.com/maven2/"

name := "couchbase-spark-samples"

organization := "com.couchbase"

version := "1.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "com.couchbase.client" %% "spark-connector" % "1.2.1",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1"
)

resolvers += Resolver.mavenLocal