package com.pavandendi.nasalogs

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("NASA Logs")
      .getOrCreate()
  }
  spark.sparkContext.setLogLevel("WARN")

}