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

package gx.flink.examples.table

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSink
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}


object KafkaTableExample {

  val TABLE_NAME = "tableName"
  val WORD_COL = "word"
  val FREQUENCY_COL = "frequency"
  val TIMESTAMP_COL = "currentTimestamp"

  def main(args: Array[String]) {
    // parse input arguments
    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 4) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]")
      return
    }

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(environment)

//    tableEnvironment
//      .connect(
//        new Kafka()
//          .version("0.9")
//          .topic(params.getRequired("input-topic"))
//          .properties(params.getProperties)
//      )
//      .withFormat(
//        new Csv()
//          .field(WORD_COL, Types.STRING)
//          .field(FREQUENCY_COL, Types.INT)
//          .field(TIMESTAMP_COL, Types.SQL_TIMESTAMP)
//      )
//      .withSchema(
//        new Schema()
//          .field(WORD_COL, Types.STRING)
//          .field(FREQUENCY_COL, Types.INT)
//          .field(TIMESTAMP_COL, Types.SQL_TIMESTAMP)
//          .proctime()
//      )
//      .inAppendMode()
//      .registerTableSource(TABLE_NAME)

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
      .registerTableSource(TABLE_NAME)


    val table = tableEnvironment.sqlQuery(
      s"""SELECT $WORD_COL, SUM($FREQUENCY_COL)
          FROM $TABLE_NAME
          GROUP BY TUMBLE($TIMESTAMP_COL, INTERVAL '30' second), $WORD_COL""")

    //    val sink = new Kafka()
    //      .version("0.9")
    //      .topic(params.getRequired("output-topic"))
    //      .properties(params.getProperties)

    val sink = new Kafka09JsonTableSink(
      params.getRequired("output-topic"),
      params.getProperties)

    table.writeToSink(sink)
    environment.execute()
  }


}
