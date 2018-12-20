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

import java.io.IOException
import java.sql.Timestamp

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

class CsvRowDeserializationSchema(typeInfo: TypeInformation[Row]) extends DeserializationSchema[Row] {

  private val fieldNames = typeInfo.asInstanceOf[RowTypeInfo].getFieldNames
  private val fieldTypes = typeInfo.asInstanceOf[RowTypeInfo].getFieldTypes

  override def deserialize(message: Array[Byte]): Row = {
    try {
      val msg = new String(message)
      val msgList = msg.split(",")
      val row = new Row(fieldNames.length)

      0 until fieldNames.length foreach { i =>
        row.setField(i, convert(msgList(i), fieldTypes(i)))
      }
      row
    } catch {
      case e: Throwable =>
        throw new IOException(s"Failed to deserialize csv record($message).", e)
    }
  }

  override def isEndOfStream(nextElement: Row): Boolean = false

  override def getProducedType: TypeInformation[Row] = typeInfo

  private def convert(value: String, info: TypeInformation[_]): Any = {
    // todo: support more types
    info match {
      case Types.BOOLEAN => value.toBoolean
      case Types.STRING => value
      case Types.INT => value.toInt
      case Types.LONG => value.toLong
      case Types.FLOAT => value.toFloat
      case Types.SQL_TIMESTAMP => new Timestamp(value.toLong)
    }

  }
}
