package gx.flink.demo.api.stream

import gx.flink.demo.util.DelayableEventSource
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object StreamJoinBroadcastState {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // broad cast userInfo stream
    val userInfo = env.fromCollection(Seq(
      UserInfo("andy", 10),
      UserInfo("bobby", 15),
      UserInfo("catty", 20)))

    val userStateDescriptor = new MapStateDescriptor(
      "UserInfoBroadcastState",
      BasicTypeInfo.STRING_TYPE_INFO,
      createTypeInformation[UserInfo]
    )

    val userBroadcastStream = userInfo.broadcast(userStateDescriptor)

    // init order stream
    val orderA = Seq(
      (1000L, Order("andy", "beer", 2)),
      (1000L, Order("bobby", "apple", 3)),
      (1000L, Order("bobby", "beer", 3)),
      (1000L, Order("catty", "apple", 4)),
      (1000L, Order("catty", "beer", 4)),
      (1000L, Order("catty", "beer", 4)))

    val order: DataStream[Order] = env.addSource(new DelayableEventSource[Order](orderA))


    // connect 2 streams and process it with BroadcastProcessFunction
    val result = order.connect(userBroadcastStream).process(new MyBroadcastProcessFunction())

    result.print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: String, product: String, amount: Int)

  case class UserInfo(user: String, age: Int)

  case class Result(user: String, product: String, amount: Int, age: Int)

  // ****************************************************************************
  //     Broadcast Process Function
  // ****************************************************************************
  class MyBroadcastProcessFunction extends BroadcastProcessFunction[Order, UserInfo, Result] {

    private final val userStateDescriptor = new MapStateDescriptor("UserInfoBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, createTypeInformation[UserInfo])

    override def processElement(value: Order, ctx: BroadcastProcessFunction[Order, UserInfo, Result]#ReadOnlyContext, out: Collector[Result]): Unit = {
      val userInfo = Option(ctx.getBroadcastState(userStateDescriptor).get(value.user)).getOrElse(UserInfo(value.user, 0))
      out.collect(Result(value.user, value.product, value.amount, userInfo.age))
    }

    override def processBroadcastElement(value: UserInfo, ctx: BroadcastProcessFunction[Order, UserInfo, Result]#Context, out: Collector[Result]): Unit = {
      ctx.getBroadcastState(userStateDescriptor).put(value.user, value)
    }

  }

}


