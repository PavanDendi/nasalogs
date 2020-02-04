package com.pavandendi.nasalogs

import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class NasaLogsSpec
    extends AnyFunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  import spark.implicits._

  describe("ingest") {

    it("downloads test data and returns a DataFrame") {

      val dataUrl = "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"

      val actualDF = NasaLogs.ingest(dataUrl)

      val expectedSchema = List(StructField("value", StringType, true))

      assert(actualDF.count === 1891715)
      assert(actualDF.schema === StructType(expectedSchema))

    }

  }

  describe("parse") {
    
    it("parses log data into correct fields") {

      val sourceDF = Seq(
        ("""199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245"""),
        ("""unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985"""),
        ("""199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085""")
      ).toDF("value")

      val actualDF = sourceDF.transform(NasaLogs.parse)

      val expectedData = List(
        Row("199.72.81.55", "01/Jul/1995:00:00:01", "/history/apollo/", 200, 6245),
        Row("unicomp6.unicomp.net", "01/Jul/1995:00:00:06", "/shuttle/countdown/", 200, 3985),
        Row("199.120.110.21", "01/Jul/1995:00:00:09", "/shuttle/missions/sts-73/mission-sts-73.html", 200, 4085)
      )

      val expectedSchema = List(
        StructField("host", StringType, true),
        StructField("timestamp", StringType, true),
        StructField("path", StringType, true),
        StructField("status", IntegerType, true),
        StructField("content_size", IntegerType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("curate") {

    it("transforms parsed data into final feature columns needed") {
      
      val sourceDF = Seq(
        ("199.72.81.55", "01/Jul/1995:00:00:01", "/history/apollo/", 200, 6245),
        ("unicomp6.unicomp.net", "01/Jul/1995:00:00:06", "/shuttle/countdown/", 200, 3985),
        ("199.120.110.21", "", "/shuttle/missions/sts-73/mission-sts-73.html", 200, 4085)
      ).toDF("host", "timestamp", "path", "status", "content_size")

      val actualDF = sourceDF.transform(NasaLogs.curate)

      val expectedDF = Seq(
        ("199.72.81.55", "/history/apollo/", "01/Jul/1995"),
        ("unicomp6.unicomp.net", "/shuttle/countdown/", "01/Jul/1995")
      ).toDF("host", "path", "date")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("rank") {

    it("ranks the top N hosts and paths from parsed log data") {
      
      val sourceDF = Seq(
        ("ip.host1.com", "/index.html", "01/Jul/1995"),
        ("ip.host1.com", "/", "01/Jul/1995"),
        ("ip.host1.com", "/index.html", "01/Jul/1995"),
        ("ip.host2.com", "/robots.txt", "01/Jul/1995"),
        ("ip.host2.com", "/index.html", "01/Jul/1995"),
        ("ip.host3.com", "/robots.txt", "01/Jul/1995")
      ).toDF("host","path","date")

      val actualDF = sourceDF.transform(NasaLogs.rank(Array("host","path"), 3))

      val expectedData = List(
        Row("01/Jul/1995", 1, "ip.host1.com", 3, "/index.html", 3),
        Row("01/Jul/1995", 2, "ip.host2.com", 2, "/robots.txt", 2),
        Row("01/Jul/1995", 3, "ip.host3.com", 1, "/", 1)
      )
      
      val expectedSchema = List(
        StructField("date", StringType, true),
        StructField("rank", IntegerType, true),
        StructField("host", StringType, true),
        StructField("count_host", IntegerType, false),
        StructField("path", StringType, true),
        StructField("count_path", IntegerType, false)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
      
      }

  }

}