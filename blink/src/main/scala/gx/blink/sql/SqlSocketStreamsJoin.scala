package gx.blink.sql

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

import scala.util.Try

object SqlSocketStreamsJoin {
  implicit val typeInfo = TypeInformation.of(classOf[TimeWord])

  def rowToRecord(row: String): (String, Long) = {
    val (year, month, day, hour) = (2019, 1, 30, 14)
    val cols = row.split(",")
    val minute = Try(cols(1).toInt).getOrElse(0)
    val second = Try(cols(2).toInt).getOrElse(0)
    val time = Timestamp.valueOf(LocalDateTime.of(year, month, day, hour, minute, second))
    //    TimeWord(cols(0), time.getTime)
    (cols(0), time.getTime)
  }

  def main(args: Array[String]): Unit = {

    // 1. get the host and the port to connect to
    var hostname: String = "localhost"
    var port1: Int = 0
    var port2: Int = 0

    try {
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
      port1 = if (params.has("port1")) params.getInt("port1") else 9001
      port2 = if (params.has("port2")) params.getInt("port2") else 9002
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port1 <port> --port2 <port>', where hostname (localhost by default) and port " +
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
    // input format:
    // word,minute,second
    val text01: DataStream[String] = env.socketTextStream(hostname, port1, '\n')
    val text1 = text01.map(rowToRecord(_))
    val textTable1 = text1.toTable(tableEnvironment)
    //    val textTable1 = tableEnvironment.fromDataStream(text1, 'word, 'time, 'pt.proctime)
    tableEnvironment.registerTable("text_table_1", textTable1)

    val text2: DataStream[String] = env.socketTextStream(hostname, port2, '\n')
    val textTable2 = tableEnvironment.fromDataStream(text2, 'word, 'pt.proctime)
    tableEnvironment.registerTable("text_table_2", textTable2)

    // 4. run query
    val sql =
      s"""SELECT t1.word, t1.time, t1.pt,
         |t2.word, t2.pt
         |FROM text_table_1 t1
         |JOIN text_table_2 t2
         |ON t1.word = t2.word
         |   AND t1.pt BETWEEN t2.pt - INTERVAL '5' SECOND AND t2.pt + INTERVAL '5' SECOND
      """.stripMargin
    val result: Table = tableEnvironment.sqlQuery(sql)

    // 5. print the results with a single thread, rather than in parallel
    result.javaStream.print().setParallelism(1)

    env.execute("Stream-Stream Join")
  }

}
