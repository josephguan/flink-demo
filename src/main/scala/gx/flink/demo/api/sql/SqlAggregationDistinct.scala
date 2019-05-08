package gx.flink.demo.api.sql

import java.sql.Timestamp
import java.util.{Calendar, Date}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._


object SqlAggregationDistinct {

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
      Order("bobby", "apple", 1, new Timestamp(now + 60 * 1000)),
      Order("catty", "beer", 1, new Timestamp(now + 60 * 2000)),
      Order("abby",  "apple", 1, new Timestamp(now + 60 * 3000)),
      Order("bobby", "beer", 1, new Timestamp(now + 60 * 4000)),

      Order("catty", "apple", 1, new Timestamp(now + 60 * 5000)),
      Order("abby",  "apple", 1, new Timestamp(now + 60 * 6000)),
      Order("bobby", "apple", 1, new Timestamp(now + 60 * 7000)),
      Order("catty", "apple", 1, new Timestamp(now + 60 * 8000)),
      Order("abby",  "apple", 1, new Timestamp(now + 60 * 9000))
    )).assignAscendingTimestamps(_.time.getTime)

    // first group by
    tEnv.registerDataStream("order_table", order, 'user, 'product, 'amount, 'ot.rowtime)

    val result = tEnv.sqlQuery(
      """SELECT TUMBLE_START(ot, INTERVAL '5' MINUTE), TUMBLE_END(ot, INTERVAL '5' MINUTE), user,
        |       COUNT(DISTINCT product) AS beer, SUM(amount) AS total
        |FROM order_table
        |GROUP BY TUMBLE(ot, INTERVAL '5' MINUTE), user
      """.stripMargin)

    result.toAppendStream[Result].print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: String, product: String, amount: Int, time: Timestamp)

  case class Result(start: Timestamp, end: Timestamp, user: String, productCnt: Long, totalAmount: Int)


}