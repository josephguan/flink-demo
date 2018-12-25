package gx.spark.examples.sql

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.SparkSession

case class TimeWord(word: String, time: Timestamp)

object SqlNetworkWordCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    // Create SparkSession
    val spark = SparkSession
      .builder
      .appName("SqlNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", args(0))
      .option("port", args(1))
      .load()

    lines.map(x => TimeWord(x.getString(0), Timestamp.valueOf(LocalDateTime.now())))
      .toDF().createOrReplaceTempView("word_table")

    val wordCounts = spark.sql(
      """SELECT word, window(time, "1 minute"), count(1)
        |FROM word_table
        |GROUP BY word, window(time, "1 minute")
      """.stripMargin)

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
