/*
 * Copyright (c) 2018 josephguan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package gx.flink.examples.streaming.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09

import scala.util.Random

object Kafka09StreamWordCountProducer {

  def main(args: Array[String]): Unit = {

    // 1. parse input arguments
    val params = ParameterTool.fromArgs(args)
    if (params.getNumberOfParameters < 4) {
      println("Missing parameters!\n"
        + "Usage: Kafka --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>"
        + "[--num-records <num>] [--throughput <messages per second>]")
      return
    }
    val recordsNum = params.getInt("num-records", 1000)
    val msgPerSecond = params.getInt("throughput", 1)

    // 2. get the ExecutionEnvironment and change some settings
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 3. create a streaming from collection
    val words = List("hello", "world", "god", "beauty", "monster")
    val random = new Random()
    val wordList = (1 to recordsNum).toStream.map(x => words(random.nextInt(5)))
    val wordStream = env.fromCollection(wordList)

    // 4. create a Kafka producer for Kafka 0.9.x
    val kafkaProducer = new FlinkKafkaProducer09(
      params.getRequired("output-topic"),
      new SimpleStringSchema,
      params.getProperties)

    wordStream.addSink(kafkaProducer)

    // finally, execute the program
    env.execute("Kafka 0.9 Json Streaming Example")
  }

}
