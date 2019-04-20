package gx.flink.table

import java.sql.Timestamp
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala._


object JoinAndGroupTableTimeWindowExample {

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
      Order(1L, "beer", 1, new Timestamp(now)),
      Order(1L, "diaper", 1, new Timestamp(now + 1000)),
      Order(1L, "diaper", 1, new Timestamp(now + 2000)),
      Order(1L, "diaper", 1, new Timestamp(now + 3000)),
      Order(1L, "diaper", 1, new Timestamp(now + 4000)),
      Order(1L, "diaper", 1, new Timestamp(now + 5000)),
      Order(3L, "rubber", 1, new Timestamp(now + 6000))
    ))
      .assignAscendingTimestamps(_.time.getTime)
      .toTable(tEnv, 'user, 'product, 'amount, 'time.rowtime)

    val userInfo = env.fromCollection(Seq(
      UserInfo(1L, "andy", 10, new Timestamp(now)),
      UserInfo(2L, "bobby", 15, new Timestamp(now + 1000)),
      UserInfo(3L, "catty", 20, new Timestamp(now + 2000))))
      .assignAscendingTimestamps(_.ut.getTime)
      .toTable(tEnv, 'id, 'name, 'age, 'ut.rowtime)

    // union the two tables
    val temp = orderA.leftOuterJoin(userInfo)
      .where('user === 'id && 'time >= 'ut - 50.seconds && 'time <= 'ut + 50.seconds)
      .select('user, 'product, 'amount, 'name, 'age, 'time)
//      .toAppendStream[Result].toTable(tEnv, 'user, 'product, 'amount, 'name, 'age, 'time.rowtime)

    val result = temp.window(Tumble over 5.seconds on 'time as 'w)
      .groupBy('w)
      .select('w.start, 'amount.sum)
      .toAppendStream[FinalResult]

    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int, time: Timestamp)

  case class UserInfo(id: Long, name: String, age: Int, ut: Timestamp)

  case class Result(user: Long, product: String, amount: Int, name: String, age: Int, time: Timestamp)

  case class FinalResult(startTime: Timestamp, amount: Int)

}