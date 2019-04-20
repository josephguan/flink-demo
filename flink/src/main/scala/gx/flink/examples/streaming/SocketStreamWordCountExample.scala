///*
// * Copyright (c) 2018 josephguan
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// */
//
//package gx.flink.examples.streaming
//
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//
//
//object SocketStreamWordCountExample {
//
//  def main(args: Array[String]): Unit = {
//
//    // 1. parse input arguments
//    val params = ParameterTool.fromArgs(args)
//    if (params.getNumberOfParameters < 1) {
//      println("Missing parameters!\n"
//        + "Usage: Example [--host <host>] --port <port> [--window <seconds>] [--slide <seconds]")
//      return
//    }
//    val host = params.get("host", "localhost")
//    val port = params.getInt("port")
//    val window = params.getInt("window", 5)
//    val slide = params.getInt("slide", -1)
//
//    // 2. get the execution environment
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // 3. get input data by connecting to the socket
//    val text = env.socketTextStream(host, port, '\n')
//
//    // 4. parse the data, group it, window it, and aggregate the counts
//    val groupedWords = text
//      .flatMap { w => w.split("\\s") }
//      .map { w => WordWithCount(w, 1) }
//      .keyBy("word")
//
//    val windowCounts = if (slide > 0) {
//      groupedWords.timeWindow(Time.seconds(window), Time.seconds(slide)).sum("count") // slide window
//    } else {
//      groupedWords.timeWindow(Time.seconds(window)).sum("count")  // tumble window
//    }
//
//    // print the results with a single thread, rather than in parallel
//    windowCounts.print().setParallelism(1)
//
//    // finally, execute the program
//    env.execute("Socket Window WordCount")
//  }
//
//  // Data type for words with count
//  case class WordWithCount(word: String, count: Long)
//
//
//}
