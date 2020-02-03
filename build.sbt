resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

name := "nasalogs"

version := "0.0.1"

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-deprecation","-feature")

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.scalatest" %% "scalatest" % "3.1.0" % "test",
    "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11"
  )

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
