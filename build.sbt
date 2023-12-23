name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.8"
fork := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.4",
  "org.apache.spark" %% "spark-sql" % "3.2.4",
//  "org.apache.spark" %% "spark-mllib" % "3.2.4",
//  "org.apache.spark" %% "spark-streaming" % "3.2.4",
//  "org.twitter4j" % "twitter4j-core" % "4.0.4",
//  "org.twitter4j" % "twitter4j-stream" % "4.0.4"
)
