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

import sbt._

object Dependencies {

  private val flinkVersion = "1.8.0"
//  private val kafkaVersion = "0.9.0.1"
//  private val sparkVersion = "2.4.0"
//  private val blinkVersion = "1.5.1"

  val flink: Seq[ModuleID] = Seq(
    "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
    "org.apache.flink" %% "flink-table-planner" % flinkVersion ,
    "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion 
//    "org.apache.flink" %% "flink-table" % flinkVersion
//    "org.apache.flink" %% "flink-connector-kafka-0.9" % flinkVersion % "provided",
//    "org.apache.flink" % "flink-json" % flinkVersion % "provided"
  )
//
//  val kafka: Seq[ModuleID] = Seq(
//    "org.apache.kafka" %% "kafka" % kafkaVersion
//  )
//
//  val spark: Seq[ModuleID] = Seq(
//    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
//    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
//    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
//    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
//  )
//
//
//
//  val blink: Seq[ModuleID] = Seq(
//    "org.apache.flink" %% "flink-scala" % blinkVersion % "provided",
//    "org.apache.flink" %% "flink-streaming-scala" % blinkVersion % "provided",
//    "org.apache.flink" %% "flink-table" % blinkVersion,
//    "org.apache.flink" %% "flink-connector-kafka-0.9" % blinkVersion % "provided",
//    "org.apache.flink" % "flink-json" % blinkVersion % "provided"
//  )

  val test: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )

}