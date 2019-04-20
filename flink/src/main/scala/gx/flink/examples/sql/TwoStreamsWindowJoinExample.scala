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
//package gx.flink.examples.sql
//
//import java.io.Serializable
//import java.sql.Timestamp
//import java.util.Random
//
///** Note: import the following packages to include some implicit values
//  * import org.apache.flink.streaming.api.scala._
//  * import org.apache.flink.table.api.scala._
//  */
//
//import gx.flink.examples.sql.WindowJoinSampleData.{GradeSource, SalarySource}
//import gx.flink.util.ThrottledIterator
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.TableEnvironment
//import org.apache.flink.table.api.scala._
//
//
//object TwoStreamsWindowJoinExample {
//
//  def main(args: Array[String]) {
//
//    // 1. set up TableEnvironment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv = TableEnvironment.getTableEnvironment(env)
//
//    // 2. create 2 streams and register them as tables
//    val salaries = env
//      .fromCollection(new ThrottledIterator(new SalarySource(), 1))
//      .toTable(tableEnv, 'name, 'salary, 'r_proctime.proctime)
//    tableEnv.registerTable("salaries", salaries)
//
//    val grades = env
//      .fromCollection(new ThrottledIterator(new GradeSource(), 1))
//      .toTable(tableEnv, 'name, 'grade, 'r_proctime.proctime)
//    tableEnv.registerTable("grades", grades)
//
//    // 3. run query
//    val sql =
//      s"""SELECT *
//          |FROM salaries a JOIN grades b
//          |ON a.name = b.name AND
//          |   a.r_proctime BETWEEN b.r_proctime - INTERVAL '5' SECOND AND b.r_proctime
//           """.stripMargin
//    val result = tableEnv.sqlQuery(sql)
//
//    // 4. print the result to log
//    result.toAppendStream[Tuple6[String, Int, Timestamp, String, Int, Timestamp]].print()
//
//    // finally, execute the program
//    env.execute("Two Stream Window Join Example")
//  }
//
//}
//
//case class Grade(name: String, grade: Int)
//
//case class Salary(name: String, salary: Int)
//
//object WindowJoinSampleData {
//
//  private val NAMES = Array("tom", "jerry", "alice", "bob", "john", "grace")
//  private val GRADE_COUNT = 5
//  private val SALARY_MAX = 10000
//
//
//  class GradeSource extends Iterator[Grade] with Serializable {
//
//    private[this] val rnd = new Random(hashCode())
//
//    def hasNext: Boolean = true
//
//    def next: Grade = {
//      Grade(NAMES(rnd.nextInt(NAMES.length)), rnd.nextInt(GRADE_COUNT) + 1)
//    }
//  }
//
//  class SalarySource extends Iterator[Salary] with Serializable {
//
//    private[this] val rnd = new Random(hashCode())
//
//    def hasNext: Boolean = true
//
//    def next: Salary = {
//      Salary(NAMES(rnd.nextInt(NAMES.length)), rnd.nextInt(SALARY_MAX) + 1)
//    }
//  }
//
//}
