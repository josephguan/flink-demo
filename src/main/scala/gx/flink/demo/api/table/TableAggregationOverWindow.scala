package gx.flink.demo.api.table

import java.sql.Timestamp
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Over
import org.apache.flink.table.api.scala._


object TableAggregationOverWindow {

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
      Order(1L, "beer", 1, new Timestamp(now)),
      Order(1L, "apple", 2, new Timestamp(now)),
      Order(2L, "beer", 3, new Timestamp(now)),
      Order(2L, "apple", 4, new Timestamp(now)),
      Order(3L, "beer", 5, new Timestamp(now)),
      Order(3L, "apple", 6, new Timestamp(now))
    )).assignAscendingTimestamps(_.time.getTime)
      .toTable(tEnv, 'user, 'product, 'amount, 'time.rowtime)

    // group by window
    val result: DataStream[Result] = orderA
      .window(Over partitionBy 'user orderBy 'time preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w)
      .select('user, 'product, 'amount, 'amount.max over 'w as 'max_amount)
      .filter('amount === 'max_amount)
      .select('user, 'product, 'amount)
      .toAppendStream[Result]

    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int, time: Timestamp)

  case class Result(user: Long, product: String, amount: Int)

}