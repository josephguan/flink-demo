package gx.flink.demo.api.table

import java.sql.Timestamp
import java.util.Calendar

import gx.flink.demo.util.DelayableEventSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala._

object TableGroupByTimeWindowWithWatermark {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)

    // init order stream
    val calendar = Calendar.getInstance()
    calendar.set(2019, 5, 8, 10, 0, 0)
    val now = calendar.getTime.getTime

    val orderA = Seq(
      (1000L, Order("andy", "beer", 2, new Timestamp(now))),
      (1000L, Order("bobby", "apple", 3, new Timestamp(now + 1000))),
      (1000L, Order("bobby", "beer", 3, new Timestamp(now + 2000))),
      (1000L, Order("catty", "apple", 4, new Timestamp(now + 3000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 4000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 5000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 6000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 7000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 8000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 9000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 2000))),  // is not discarded in the first window
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 10000))), // emit a watermark, t = now + 5000
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 2000))),  // is discarded in the first window
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 11000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 12000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 8000))),  // is not discarded in the second window
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 15000))), // emit a watermark, t = now + 10000
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 8000))),  // is discarded in the second window
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 16000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 17000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 18000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 19000))),
      (1000L, Order("catty", "beer", 4, new Timestamp(now + 20000))), // emit a watermark, t = now + 15000
      (5000L, Order("catty", "apple", 9, new Timestamp(now + 21000))))

    val order = env
      .addSource(new DelayableEventSource[Order](orderA))
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(5)) {
          override def extractTimestamp(element: Order): Long = element.eventTime.getTime
        })

    val orderTable = order.toTable(tEnv, 'user, 'product, 'amount, 'eventTime.rowtime)

    val result: DataStream[Result] = orderTable.window(Tumble over 5.seconds on 'eventTime as 'w)
      .groupBy('w)
      .select('w.start, 'user.count, 'w.proctime)
      .toAppendStream[Result]

    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: String, product: String, amount: Int, eventTime: Timestamp)

  case class Result(start: Timestamp, count: Long, procTime: Timestamp)

}
