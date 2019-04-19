package gx.blink.sql

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

object SqlSocketWordCount {

  def main(args: Array[String]): Unit = {

    // 1. get the host and the port to connect to
    var hostname: String = "localhost"
    var port: Int = 0

    try {
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
      port = params.getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
          "is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l <port>' " +
          "and type the input text into the command line")
        return
      }
    }

    // 2. get the table environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(env)


    // 3. get input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')
    val textTable = tableEnvironment.fromDataStream(text, 'word, 'pt.proctime)
    tableEnvironment.registerTable("text_table", textTable)

    // 4. run query
    val interval = 5
    val sql =
      s"""SELECT word,
         |TUMBLE_START(pt, INTERVAL '$interval' second),
         |TUMBLE_END(pt, INTERVAL '$interval' second),
         |COUNT(word)
         |FROM text_table
         |GROUP BY TUMBLE(pt, INTERVAL '$interval' second), word
      """.stripMargin
    val result: Table = tableEnvironment.sqlQuery(sql)

    // 5. print the results with a single thread, rather than in parallel
    result.printSchema()
    result.javaStream.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

}
