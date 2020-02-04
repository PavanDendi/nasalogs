package com.pavandendi.nasalogs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkFiles


/**
 * Downloads log data from a url and ranks the top N results.
 * Specify the url and number of results as an argument during spark-submit.
 * 
 * Example `spark-submit` command:
 * {{{
 * spark-submit --class com.pavandendi.nasalogs.NasaLogs \
 * /path/to/jar/nasalogs_2.11-0.0.1.jar \
 * ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 3
 * }}}
 */
object NasaLogs extends SparkSessionWrapper {

  import spark.implicits._

  def main(args: Array[String]) = {

    // Command line spark-submit arguments
    val dataUrl = args(0)
    val topN = args(1).toInt

    val ingestDF = ingest(dataUrl)
    
    // All custom logic is implemented using separate functions
    // and chained using the transform method.
    // This enables easier unit testing.
    val rankedDF = ingestDF.transform(parse)
                           .transform(curate)
                           .transform(rank(Array("host","path"), topN))
    rankedDF.show(300, false)

  }

  /**
   * Downloads log data and returns a Dataframe with the raw unprocessed data.
   *
   * @param dataUrl URL to download data from
   */
  def ingest(dataUrl: String): DataFrame = {

    spark.sparkContext.addFile(dataUrl)
    spark.read.text(SparkFiles.get(dataUrl.split("/").last))
  
  }

  /**
   * Parses raw log data and extracts meaningful fields.
   * Not all fields are required for final output, but they are preserved
   * in this function for possible future use.
   *
   * Regex modified from article "Web Server Log Analysis with Spark", part (2b)  
   * https://adataanalyst.com/spark/web-server-log-analysis-spark/
   *
   * @param rawDF Dataframe that contains the raw log data
   */
  def parse(rawDF: DataFrame): DataFrame = {
  
    rawDF.select(
            regexp_extract($"value", """^([^(\s|,)]+)""", 1).as("host"),
            regexp_extract($"value", """^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).as("timestamp"),
            regexp_extract($"value", """^.*\w+\s+([^\s]+)\s+HTTP.*""",1).as("path"),
            regexp_extract($"value", """^.*"\s+([^\s]+)""", 1).cast("integer").as("status"),
            regexp_extract($"value", """^.*\s+(\d+)$""", 1).cast("integer").as("content_size")
          )
  
  }

  /**
   * Prepares parsed log data to be ranked
   *
   * @param df Dataframe that contains parsed log data
   */
  def curate(df: DataFrame): DataFrame = {

    df.drop("status","content_size")
      .filter($"timestamp" =!= "")
      .withColumn("date",$"timestamp".substr(1,11))
      .drop("timestamp")

  }

  /**
   * Ranks log data and returns the top results for each day.
   * Accepts arbitrary number of columns to rank.
   *
   * @param colNames Array of column names to be ranked
   * @param topN How many results to show for each day
   * @param df Dataframe that contains parsed log data
   */
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
