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

package gx.flink.customize.sources

import java.util.Properties

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaConsumerBase}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.types.Row


class Kafka09CsvTableSource(topic: String, properties: Properties, tableSchema: TableSchema)
  extends KafkaCsvTableSource(topic, properties, tableSchema) {

  override def createKafkaConsumer(topic: String, properties: Properties, deserializationSchema: DeserializationSchema[Row]): FlinkKafkaConsumerBase[Row] = {
      new FlinkKafkaConsumer09[Row](topic, deserializationSchema, properties)
  }

  // todo: make a builder to support set time attribute column

}
