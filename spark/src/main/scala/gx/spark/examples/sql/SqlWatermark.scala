package gx.spark.examples.sql

import java.sql.Timestamp
import java.time.LocalDateTime

import gx.spark.examples.util.TimeWord
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

object SqlWatermark {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SqlWatermark <hostname1> <port1> <watermark> <window>")
      System.exit(1)
    }

    // Create SparkSession
    val spark = SparkSession
      .builder
      .appName("SqlWatermark")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val stream = spark.readStream
      .format("socket")
      .option("host", args(0))
      .option("port", args(1))
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

    stream.map(rowToRecord)
      .withWatermark("time", s"${args(2)} seconds")
      .toDF().createOrReplaceTempView("stream1")

    val select = spark.sql(
      s"""SELECT word, window(time, "${args(3)} seconds"), count(1)
         |FROM stream1
         |GROUP BY word, window(time, "${args(3)} seconds")
          """.stripMargin)

    // Start running the query that prints the running counts to the console
    val query = select.writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      //      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }

}
