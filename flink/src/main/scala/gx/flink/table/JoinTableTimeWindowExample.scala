package gx.flink.table

import java.sql.Timestamp
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._


object JoinTableTimeWindowExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    val now = new Date().getTime


    val orderA = env.fromCollection(Seq(
      Order(1L, "beer", 3, new Timestamp(now)),
      Order(1L, "diaper", 4, new Timestamp(now + 10000)),
      Order(3L, "rubber", 2, new Timestamp(now + 20000))))
      .assignAscendingTimestamps(_.time.getTime)
      .toTable(tEnv, 'user, 'product, 'amount, 'time.rowtime)

    val userInfo = env.fromCollection(Seq(
      UserInfo(1L, "andy", 10, new Timestamp(now)),
      UserInfo(2L, "bobby", 15, new Timestamp(now + 10000)),
      UserInfo(3L, "catty", 20, new Timestamp(now + 10000))))
      .assignAscendingTimestamps(_.ut.getTime)
      .toTable(tEnv, 'id, 'name, 'age, 'ut.rowtime)

    // union the two tables
    val result: DataStream[Result] = orderA.leftOuterJoin(userInfo)
      .where('user==='id && 'time >= 'ut - 5.seconds && 'time <= 'ut + 5.seconds)
      .select('user, 'product, 'amount, 'name, 'age, 'time)
      .toAppendStream[Result]

    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int, time: Timestamp)
  case class UserInfo(id: Long, name: String, age: Int, ut: Timestamp)
  case class Result(user: Long, product: String, amount: Int, name: String, age: Int, time: Timestamp)

}