package com.pavandendi.nasalogs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkFiles

object NasaLogs extends SparkSessionWrapper {

  import spark.implicits._

  def main(args: Array[String]) = {

    val dataUrl = args(0)
    val topN = args(1).toInt

    val ingestDF = ingest(dataUrl)
    
    val rankedDF = ingestDF.transform(parse)
                           .transform(curate)
                           .transform(rank(Array("host","path"), topN))
    rankedDF.show(300, false)

  }

  def ingest(dataUrl: String): DataFrame = {

    spark.sparkContext.addFile(dataUrl)
    spark.read.text(SparkFiles.get(dataUrl.split("/").last))
  
  }

  def parse(rawDF: DataFrame): DataFrame = {
  
    rawDF.select(
            regexp_extract($"value", """^([^(\s|,)]+)""", 1).as("host"),
            regexp_extract($"value", """^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).as("timestamp"),
            regexp_extract($"value", """^.*\w+\s+([^\s]+)\s+HTTP.*""",1).as("path"),
            regexp_extract($"value", """^.*"\s+([^\s]+)""", 1).cast("integer").as("status"),
            regexp_extract($"value", """^.*\s+(\d+)$""", 1).cast("integer").as("content_size")
          )
  
  }

  def curate(df: DataFrame): DataFrame = {

    df.drop("status","content_size")
      .filter($"timestamp" =!= "")
      .withColumn("date",$"timestamp".substr(1,11))
      .drop("timestamp")

  }

  def rank(colNames: Array[String], topN: Integer)(df: DataFrame): DataFrame = {
  
    (for (colName <- colNames) yield {

      df.groupBy("date",colName).count
        .withColumn(s"count_${colName}", $"count".cast("integer")).drop("count")
        .withColumn("rank", row_number().over(Window.partitionBy("date").orderBy(col(s"count_${colName}").desc)))
        .filter($"rank" <= topN)
    
    }).reduce(_.join(_, Seq("date","rank")))
      .orderBy("date","rank")
  
  }

}
