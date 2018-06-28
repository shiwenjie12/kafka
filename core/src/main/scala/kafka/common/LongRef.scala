/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.common

/**
  * 一个可变单元格，其中包含“Long”类型的值。
  * 一般应该更喜欢使用基于值的编程（即传递和返回“Long”值），但是这个类在某些场景中可能很有用。
  * 与AtomicLong不同，这个类不是线程安全的，并且没有原子性保证。
  */
class LongRef(var value: Long) {

  def addAndGet(delta: Long): Long = {
    value += delta
    value
  }

  def getAndAdd(delta: Long): Long = {
    val result = value
    value += delta
    result
  }

  def getAndIncrement(): Long = {
    val v = value
    value += 1
    v
  }

  def incrementAndGet(): Long = {
    value += 1
    value
  }

  def getAndDecrement(): Long = {
    val v = value
    value -= 1
    v
  }

  def decrementAndGet(): Long = {
    value -= 1
    value
  }

}
