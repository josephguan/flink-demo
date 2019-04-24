package gx.flink.demo.api.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._


object TableJoin {

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

    val userInfo = env.fromCollection(Seq(
      UserInfo(1L, "andy", 10),
      UserInfo(2L, "bobby", 15),
      UserInfo(3L, "catty", 20))).toTable(tEnv)

    // join the two tables
    val result: DataStream[Result] = orderA.leftOuterJoin(userInfo)
      .where('user==='id)
      .select('user, 'product, 'amount, 'name, 'age)
      .toAppendStream[Result]

    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int)
  case class UserInfo(id: Long, name: String, age: Int)
  case class Result(user: Long, product: String, amount: Int, name: String, age: Int)

}