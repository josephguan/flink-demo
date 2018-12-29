package gx.spark.examples.sql

import java.sql.Timestamp
import java.time.LocalDateTime

import gx.spark.examples.util.TimeWord
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

object SqlJoinWithStream {

  def main(args: Array[String]) {
    if (args.length < 7) {
      System.err.println("Usage: SqlJoinWithStream <hostname1> <port1> <watermark1> <hostname2> <port2> <watermark2> <delay>")
      System.exit(1)
    }

    // Create SparkSession
    val spark = SparkSession
      .builder
      .appName("SqlJoinWithStream")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val stream1 = spark.readStream
      .format("socket")
      .option("host", args(0))
      .option("port", args(1))
      .load()

    val stream2 = spark.readStream
      .format("socket")
      .option("host", args(3))
      .option("port", args(4))
      .load()

    // input format:
    // word,minute,second
    def rowToRecord(row: Row): TimeWord = {
      val (year, month, day, hour) = (2018, 12, 30, 14)
      val cols = row.getString(0).split(",")
      val minute = Try(cols(1).toInt).getOrElse(0)
      val second = Try(cols(2).toInt).getOrElse(0)
      val time = Timestamp.valueOf(LocalDateTime.of(year, month, day, hour, minute, second))
      TimeWord(cols(0), time)
    }

    stream1.map(rowToRecord)
      .toDF()
      .withWatermark("time", s"${args(2)} second")
      .createOrReplaceTempView("stream1")

    stream2.map(rowToRecord)
      .toDF()
      .withWatermark("time", s"${args(5)} second")
      .createOrReplaceTempView("stream2")


    val joins = spark.sql(
      s"""SELECT a.word, a.time, b.word, b.time
        |FROM stream1 a
        |LEFT JOIN stream2 b
        |ON a.word=b.word
        |AND a.time BETWEEN b.time - INTERVAL ${args(6)} SECOND AND b.time
      """.stripMargin)

    // Start running the query that prints the running counts to the console
    val query = joins.writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      .start()

    query.awaitTermination()
  }

}
