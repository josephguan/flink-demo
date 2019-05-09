package gx.flink.demo.util

import org.apache.flink.streaming.api.functions.source.SourceFunction

class DelayableEventSource[T](data: Seq[(Long, T)]) extends SourceFunction[T] {
  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    data.foreach { case (delay, item) =>
      Thread.sleep(delay)
      ctx.collect(item)
    }
  }

  override def cancel(): Unit = {}

}
