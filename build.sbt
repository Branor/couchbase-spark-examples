name := "couchbase-spark-examples"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.couchbase.client" %% "spark-connector" % "1.0.0-dp",
  "org.apache.spark" %% "spark-streaming" % "1.2.1"
)

resolvers += "Couchbase Repository" at "http://files.couchbase.com/maven2/"