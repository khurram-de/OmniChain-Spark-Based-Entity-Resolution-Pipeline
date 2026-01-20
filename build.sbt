name := "omnichain"
version := "0.1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided",
  "com.typesafe" % "config" % "1.4.3",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)
