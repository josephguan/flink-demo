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

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.kafka.clients.consumer.ConsumerConfig


// Data type for words with count
case class WordWithCount(word: String, count: Long)


object KafkaExample {

  def main(args: Array[String]): Unit = {
    // create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    println("joe: create execution environment")

    // parse user parameters
    val params: ParameterTool = ParameterTool.fromArgs(args)

    // create kafka consumer
    val kafkaProperties = createKafkaConsumerConfig(
      params.getRequired("bootstrap.servers"),
      params.get("group.id", "flink_demo"),
      params.getRequired("zookeeper.connect"))
    println(s"joe: $kafkaProperties")

    val kafkaConsumer = new FlinkKafkaConsumer09[String](
      params.getRequired("input-topic"),
      new SimpleStringSchema(),
      kafkaProperties
    )
    println(s"joe: $kafkaConsumer")

    // create stream
    val stream: DataStream[String] = env.addSource(kafkaConsumer)

    // word count: parse the data, group it, window it, and aggregate the counts
    //    val windowCounts = stream
    //      .flatMap { w => w.split("\\s") }
    //      .map { w => WordWithCount(w, 1) }
    //      .keyBy("word")
    //      .timeWindow(Time.seconds(5), Time.seconds(1))
    //      .sum("count")
    //
    //    // print the results with a single thread, rather than in parallel
    //    windowCounts.print().setParallelism(1)

    // create a Kafka producer for Kafka 0.10.x
    val kafkaProducer = new FlinkKafkaProducer09(
      params.getRequired("output-topic"),
      new SimpleStringSchema,
      params.getProperties)

    // write data into Kafka
    stream.addSink(kafkaProducer)

    stream.map(x => "joe: " + x).print()

    println(s"joe: env.execute()")
    env.execute()


    //    val kafkaProducer = new FlinkKafkaProducer[String](
    //      "localhost:9092",
    //      "output",
    //      KafkaStringSchema
    //    )

    //    val wordCounts = countWords(lines, stopWords, window)
    //
    //    wordCounts
    //      .map(_.toString)
    //      .addSink(kafkaProducer)
    //
    //    env.execute()
  }

  def createKafkaConsumerConfig(brokers: String, groupId: String, zookeeper: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put("zookeeper.connect", zookeeper)
    //    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    //    props.put(ConbsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    //    props.put(ConsumerConfig.SESSION_TIMEOUT_MS, "30000")
    //    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    //    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

}
