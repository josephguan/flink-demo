package gx.flink.demo.api.table

import java.sql.Timestamp
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction

class MySplitUDTF(lineSep: String, fieldSep: String) extends TableFunction[(Long, String, Int)] {
  def eval(str: String): Unit = {
    // use collect(...) to emit a row.
    str.split(lineSep).foreach { line =>
      val fields = line.split(fieldSep)
      collect((fields(0).toLong, fields(1), fields(2).toInt))
    }
  }
}


object TableJoinDimensionOld {

  // *************************************************************************
  //  PROGRAM:  use UDTF to implement join dimension table
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)

    val now = new Date().getTime
    val orderA = env.fromCollection(Seq(
      Order(1L, "beer", 3, new Timestamp(now - 10000)),
      Order(1L, "diaper", 4, new Timestamp(now - 10000)),
      Order(3L, "apple", 5, new Timestamp(now - 20000)),
      Order(3L, "apple", 6, new Timestamp(now - 30000)),
      Order(2L, "apple", 7, new Timestamp(now - 40000)),
      Order(2L, "apple", 6, new Timestamp(now - 20000)),
      Order(4L, "rubber", 2, new Timestamp(now - 30000))))
      .assignAscendingTimestamps(_.time.getTime)
      .toTable(tEnv, 'user, 'product, 'amount, 'time.rowtime)

    val userInfoCsv: String = "1,andy,10\n2,bobby,15"

    // user defined table function
    val split = new MySplitUDTF("\n",",")

    // join the two tables
    val result: DataStream[Result] = orderA.leftOuterJoinLateral(split(userInfoCsv) as ('id, 'name, 'age))
      .where('user === 'id)
      .select('user, 'product, 'amount, 'name, 'age)
      .toAppendStream[Result]

    /** ***********************************************************
      * Problems:
      *   it only supports inner joins currently.
      * ***********************************************************/
    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int, time: Timestamp)

  case class UserInfo(id: Long, name: String, age: Int)

  case class Result(user: Long, product: String, amount: Int, name: String, age: Int)

}