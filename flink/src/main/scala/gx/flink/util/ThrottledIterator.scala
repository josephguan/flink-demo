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

/**
  * Refer To:
  * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/utils/ThrottledIterator.java
  */

package gx.flink.util

class ThrottledIterator[T](source: Iterator[T], elementsPerSecond: Int) extends Iterator[T] with Serializable {

  private val (sleepBatchSize, sleepBatchTime) = {
    if (elementsPerSecond >= 100) (elementsPerSecond / 20, 50)
    else if (elementsPerSecond >= 1) (1, 1000 / elementsPerSecond)
    else throw new IllegalArgumentException("'elements per second' must be positive and not zero")
  }

  private var lastBatchCheckTime: Long = 0
  private var num: Int = 0

  override def hasNext: Boolean = source.hasNext

  override def next(): T = {
    if (lastBatchCheckTime > 0) {
      num += 1
      if (num >= sleepBatchSize) {
        num = 0

        val now = System.currentTimeMillis()
        val elapsed = now - lastBatchCheckTime
        if (elapsed < sleepBatchTime) {
          try {
            Thread.sleep(sleepBatchTime - elapsed)
          } catch {
            case e: InterruptedException =>
              // restore interrupt flag and proceed
              Thread.currentThread().interrupt();
          }
        }
        lastBatchCheckTime = now
      }
    } else {
      lastBatchCheckTime = System.currentTimeMillis()
    }

    source.next()
  }

}
