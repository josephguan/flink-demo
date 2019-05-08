package gx.flink.demo.api.sql

import java.sql.Timestamp
import java.util.{Calendar, Date}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._


object SqlGroupByTimeWindow2 {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)

    // set up tables
    val calendar = Calendar.getInstance()
    calendar.set(2019, 5, 8, 10, 0, 0)
    val now = calendar.getTime.getTime

    val order = env.fromCollection(Seq(
      Order("abby",  "apple", 1, new Timestamp(now)),
      Order("bobby", "beer", 1, new Timestamp(now + 60 * 1000)),
      Order("catty", "beer", 1, new Timestamp(now + 60 * 2000)),
      Order("abby",  "apple", 1, new Timestamp(now + 60 * 3000)),
      Order("bobby", "beer", 1, new Timestamp(now + 60 * 4000)),
      Order("catty", "beer", 1, new Timestamp(now + 60 * 5000)),
      Order("abby",  "apple", 1, new Timestamp(now + 60 * 6000)),
      Order("bobby", "beer", 1, new Timestamp(now + 60 * 7000)),
      Order("catty", "beer", 1, new Timestamp(now + 60 * 8000)),
      Order("abby",  "beer", 1, new Timestamp(now + 60 * 9000)),
      Order("bobby", "beer", 1, new Timestamp(now + 60 * 10000)),
      Order("catty", "beer", 1, new Timestamp(now + 60 * 11000))
    )).assignAscendingTimestamps(_.time.getTime)

    // first group by
    tEnv.registerDataStream("order_table", order, 'user, 'product, 'amount, 'ot.rowtime)

    val resultFor5Min = tEnv.sqlQuery(
      """SELECT TUMBLE_START(ot, INTERVAL '5' MINUTE), TUMBLE_END(ot, INTERVAL '5' MINUTE), user,
        |       SUM(CASE WHEN product = 'beer' THEN 1 ELSE 0 END) AS beer, SUM(amount) AS total,
        |       TUMBLE_ROWTIME(ot, INTERVAL '5' MINUTE) as rt
        |FROM order_table
        |GROUP BY TUMBLE(ot, INTERVAL '5' MINUTE), user
      """.stripMargin)

    // second group by
    tEnv.registerTable("order_5_min", resultFor5Min)

    val resultFor10Min = tEnv.sqlQuery(
      """SELECT TUMBLE_START(rt, INTERVAL '10' MINUTE), TUMBLE_END(rt, INTERVAL '10' MINUTE), user,
        |       SUM(beer) AS beer, SUM(total) AS total
        |FROM order_5_min
        |GROUP BY TUMBLE(rt, INTERVAL '10' MINUTE), user
      """.stripMargin)

    resultFor5Min.toAppendStream[Result5Min].print()
    resultFor10Min.toAppendStream[Result10Min].print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: String, product: String, amount: Int, time: Timestamp)

  case class Result5Min(start: Timestamp, end: Timestamp, user: String, beerAmount: Int, totalAmount: Int, rowTime: Timestamp)

  case class Result10Min(start: Timestamp, end: Timestamp, user: String, beerAmount: Int, totalAmount: Int)


}