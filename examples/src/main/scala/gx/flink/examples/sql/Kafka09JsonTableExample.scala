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

package gx.flink.examples.sql

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}


object Kafka09JsonTableExample {

  val InputTable = "input_table"
  val OutputTable = "output_table"
  val WORD_COL = "word"
  val FREQUENCY_COL = "frequency"
  val TIMESTAMP_COL = "currentTimestamp"

  def main(args: Array[String]) {

    // 1. parse input arguments
    val params = ParameterTool.fromArgs(args)
    if (params.getNumberOfParameters < 5) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id> "
        + "[--interval <second>]")
      return
    }
    val interval = params.getInt("interval", 5)

    // 2. create a TableEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(env)

    // 3. register a TableSource for kafka topic
    tableEnvironment
      .connect(
        new Kafka()
          .version("0.9")
          .topic(params.getRequired("input-topic"))
          .properties(params.getProperties)
      )
      .withFormat(
        new Json()
          .deriveSchema()
      )
      .withSchema(new Schema()
        .field(WORD_COL, Types.STRING)
        .field(FREQUENCY_COL, Types.INT)
        .field(TIMESTAMP_COL, Types.SQL_TIMESTAMP)
        .proctime()
      )
      .inAppendMode()
      .registerTableSource(InputTable)

    // 4.register a TableSink for result
    tableEnvironment
      .connect(
        new Kafka()
          .version("0.9")
          .topic(params.getRequired("output-topic"))
          .properties(params.getProperties)
      )
      .withFormat(
        new Json()
          .deriveSchema()
      )
      .withSchema(new Schema()
        .field(WORD_COL, Types.STRING)
        .field("start_time", Types.SQL_TIMESTAMP)
        .field("end_time", Types.SQL_TIMESTAMP)
        .field(FREQUENCY_COL, Types.INT)
      )
      .inAppendMode()
      .registerTableSink(OutputTable)

    // 5. run query
    val sql =
      s"""SELECT $WORD_COL,
          |TUMBLE_START($TIMESTAMP_COL, INTERVAL '$interval' second),
          |TUMBLE_END($TIMESTAMP_COL, INTERVAL '$interval' second),
          |SUM($FREQUENCY_COL)
          |FROM $InputTable
          |GROUP BY TUMBLE($TIMESTAMP_COL, INTERVAL '$interval' second), $WORD_COL
      """.stripMargin
    val result = tableEnvironment.sqlQuery(sql)

    // 6. Emit the result to the registered TableSink
    result.insertInto(OutputTable)

    // finally, execute the program
    env.execute("Kafka 0.9 Json Table Example")
  }

}