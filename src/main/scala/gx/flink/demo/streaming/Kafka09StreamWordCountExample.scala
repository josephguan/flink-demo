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
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
//
//object Kafka09StreamWordCountExample {
//
//  def main(args: Array[String]): Unit = {
//
//    // 1. parse input arguments
//    val params = ParameterTool.fromArgs(args)
//    if (params.getNumberOfParameters < 4) {
//      println("Missing parameters!\n"
//        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
//        + "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>")
//      return
//    }
//
//    // 2. get the ExecutionEnvironment and change some settings
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.disableSysoutLogging
//    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
//    env.enableCheckpointing(5000) // create a checkpoint every 5 seconds
//    env.getConfig.setGlobalJobParameters(params) // make parameters available in the web interface
//
//    // 3. create a Kafka streaming source consumer for Kafka 0.9.x
//    val kafkaConsumer = new FlinkKafkaConsumer09(
//      params.getRequired("input-topic"),
//      new SimpleStringSchema,
//      params.getProperties)
//
//    // 4. get the input streaming
//    val messageStream = env.addSource(kafkaConsumer)
//
//    // 5. word counting
//    val windowCounts = messageStream
//      .flatMap { w => w.split("\\s") }
//      .map { w => (w, 1) }
//      .keyBy(0) // key stream by word
//      .timeWindow(Time.seconds(5)) // tumbling time window of 1 minute length
//      .sum(1) // compute sum over carCnt
//
//    // 6. print the results with a single thread, rather than in parallel
//    windowCounts.print().setParallelism(1)
//
//    // 7. create a Kafka producer for Kafka 0.9.x
//    val kafkaProducer = new FlinkKafkaProducer09(
//      params.getRequired("output-topic"),
//      new SimpleStringSchema,
//      params.getProperties)
//
//    // 8. write data into Kafka
//    windowCounts.map(x => s"${x._1},${x._2}").addSink(kafkaProducer)
//
//    // finally, execute the program
//    env.execute("Kafka 0.9 Json Streaming Example")
//  }
//
//}
