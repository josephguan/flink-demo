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

package gx.kafka.util

import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ArgParser {

  private type ParaMap = mutable.LinkedHashMap[String, ListBuffer[String]]

  private val paraMap = new ParaMap()

  def parse(args: Array[String]): Unit = {
    args.foldLeft(paraMap) { (pm, arg) =>
      if (arg.startsWith("--")) {
        pm.put(arg.substring(2), new ListBuffer())
        pm
      } else {
        if (pm.nonEmpty) {
          pm.last._2.append(arg)
        }
        pm
      }
    }
  }

  def getProperties: Properties = {
    val props: Properties = new Properties
    props.putAll(getParaMap)
    props
  }

  def getParaMap: Map[String, String] = {
    paraMap.map { case (key, values) =>
      key -> valueToString(values)
    }.toMap
  }

  def get(key: String): Option[String] = {
    paraMap.get(key).map(valueToString)
  }

  def getInt(key: String): Option[Int] = {
    paraMap.get(key).flatMap(values => values.headOption.map(_.toInt))
  }

  def getLong(key: String): Option[Long] = {
    paraMap.get(key).flatMap(values => values.headOption.map(_.toLong))
  }

  private def valueToString(values: ListBuffer[String]): String = {
    values.mkString(" ")
  }
}

object ArgParser {
  def apply(args: Array[String]): ArgParser = {
    val parser = new ArgParser()
    parser.parse(args)
    parser
  }
}