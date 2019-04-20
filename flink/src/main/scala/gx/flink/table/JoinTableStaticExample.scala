package gx.flink.table

import java.sql.Timestamp
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._


object JoinTableStaticExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)

    // set up tables
    val now = new Date().getTime


    val userInfoTable = env.fromCollection(Seq(
      UserInfo(1L, "andy", 10),
      UserInfo(2L, "bobby", 15),
      UserInfo(3L, "catty", 20)))
      .toTable(tEnv, 'id, 'name, 'age, 'r_proctime.proctime)

    val orderA = env.fromCollection(Seq(
      Order(1L, "beer", 3, new Timestamp(now - 10000)),
      Order(1L, "diaper", 4, new Timestamp(now - 10000)),
      Order(3L, "apple", 5, new Timestamp(now - 20000)),
      Order(3L, "apple", 6, new Timestamp(now - 30000)),
      Order(2L, "apple", 7, new Timestamp(now - 40000)),
      Order(2L, "apple", 6, new Timestamp(now - 20000)),
      Order(3L, "rubber", 2, new Timestamp(now - 30000))))
      .assignAscendingTimestamps(_.time.getTime)
      .toTable(tEnv, 'user, 'product, 'amount, 'time.rowtime, 'o_proctime.proctime)


    // create temporal table function
    val userInfo = userInfoTable
      .createTemporalTableFunction('r_proctime, 'id)

    // join with temporal table, which can be treated as dimension table
    val result: DataStream[Result] = orderA.joinLateral(userInfo('o_proctime), 'user === 'id)
      .select('user, 'product, 'amount, 'name, 'age, 'time, 'o_proctime, 'r_proctime)
      .toAppendStream[Result]

    result.print()

    // force printing orderA table to make its proctime a little bit latter than userInfoTable
    orderA.toAppendStream[OrderTimed].print()
    userInfoTable.toAppendStream[UserInfoTimed].print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int, time: Timestamp)

  case class OrderTimed(user: Long, product: String, amount: Int, time: Timestamp, pt: Timestamp)

  case class UserInfo(id: Long, name: String, age: Int)

  case class UserInfoTimed(id: Long, name: String, age: Int, pt: Timestamp)

  case class Result(user: Long, product: String, amount: Int, name: String, age: Int, time: Timestamp,
                    o_proctime: Timestamp, r_proctime: Timestamp)

}