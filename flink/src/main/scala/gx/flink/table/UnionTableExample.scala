package gx.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._

/**
  * Simple example for demonstrating the use of Table API on a Stream Table.
  *
  * This example shows how to:
  *  - Convert DataStreams to Tables
  *  - Apply union, select, and filter operations
  */
object UnionTableExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val orderA = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2))).toTable(tEnv)

    val orderB = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1))).toTable(tEnv)

    // union the two tables
    val result: DataStream[Order] = orderA.unionAll(orderB)
      .select('user, 'product, 'amount)
      .where('amount > 2)
      .toAppendStream[Order]

    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int)

}