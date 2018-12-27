package gx.spark.examples.sql

import org.apache.spark.sql.SparkSession

object SqlJoinWithStatic {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SqlJoinWithStatic <hostname> <port>")
      System.exit(1)
    }

    // Create SparkSession
    val spark = SparkSession
      .builder
      .appName("SqlJoinWithStatic")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", args(0))
      .option("port", args(1))
      .load()

    lines.map(x => x.getString(0))
      .toDF("word").createOrReplaceTempView("word_table")

    val codes = Seq(
      ("a", "abby"),
      ("b", "bobby"),
      ("c", "catty"),
      ("d", "dotty"),
      ("", "empty")
    ).toDF("id", "name")
    codes.createOrReplaceTempView("code_table")


    val wordCounts = spark.sql(
      """SELECT word, id, name
        |FROM word_table a
        |LEFT JOIN code_table b
        |ON a.word = b.id
      """.stripMargin)

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
