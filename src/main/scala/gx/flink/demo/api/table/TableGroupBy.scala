package gx.flink.demo.api.table

import java.sql.Timestamp
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala._


object TableGroupBy{

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    val now = new Date().getTime

    // set up tables
    val orderA = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "beer", 3),
      Order(1L, "apple", 3),
      Order(2L, "apple", 4),
      Order(2L, "beer", 2))).toTable(tEnv)

    // group by user and product
    // output table is not an append-only table. Use the toRetractStream() in order to handle add and retract messages.
    val result: DataStream[(Boolean, Result)] = orderA
      .groupBy('user, 'product)
      .select('user, 'product, 'amount.sum)
      .toRetractStream[Result]

    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int)

  case class Result(user: Long, product: String, total: Int)

}