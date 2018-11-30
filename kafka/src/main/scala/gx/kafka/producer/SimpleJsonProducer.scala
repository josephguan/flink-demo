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

package gx.kafka.producer

import java.util.Properties
import java.util.Date


import gx.kafka.util._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random


object SimpleJsonProducer {

  def main(args: Array[String]) {

    // 1. parse and get parameters
    val params = ArgParser(args)
    val topic = params.get("topic").get
    val records = params.getInt("records").getOrElse(1000)
    val throughput = params.getInt("throughput").getOrElse(1)

    // 2. set default properties
    val props: Properties = params.getProperties
    setDefault(props, "client.id", "SimpleJsonProducer")
    setDefault(props, "key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    setDefault(props, "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 3. create a KafkaProducer
    val producer = new KafkaProducer[String, String](props)

    // 4. send records
    val random = new Random()
    val words = List("hello", "world", "god", "beauty", "monster", "test", "good", "funny")
    val len = words.length
    val interval: Int = 1000 / throughput

    (1 to records).foreach { i =>
      val word = words(random.nextInt(len))
      val frequency = random.nextInt(255)
      val runtime = new Date().getTime
      val msg = s"""{"word":"$word","frequency":$frequency,"currentTimestamp":$runtime}"""
      val data = new ProducerRecord[String, String](topic, word, msg)
      producer.send(data)
      println(s"$msg")
      if (interval != 0) Thread.sleep(interval)
    }

    // 5. close producer
    System.out.println("Producer finished sending messages...")
    producer.close()
  }


}
