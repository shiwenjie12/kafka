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
package kafka.utils.timer

import java.util.concurrent.{Delayed, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Time

import scala.math._

// 环形结构
@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  private[this] val root = new TimerTaskEntry(null, -1)
  root.next = root
  root.prev = root

  // 过期时间
  private[this] val expiration = new AtomicLong(-1L)

  // Set the bucket's expiration time
  // Returns true if the expiration time is changed
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time
  def getExpiration(): Long = {
    expiration.get()
  }

  // Apply the supplied function to each of tasks in this list
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next

        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    while (!done) {
      // Remove the timer task entry if it is already in any other list
      // We do this outside of the sync block below to avoid deadlocking.
      // We may retry until timerTaskEntry.list becomes null.
      timerTaskEntry.remove()

      synchronized {
        timerTaskEntry.synchronized {
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            // 添加到列头
            val tail = root.prev
            timerTaskEntry.next = root
            timerTaskEntry.prev = tail
            timerTaskEntry.list = this
            tail.next = timerTaskEntry
            root.prev = timerTaskEntry
            taskCounter.incrementAndGet()
            done = true
          }
        }
      }
    }
  }

  // 从list中移除特殊的时间的任务实体
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        if (timerTaskEntry.list eq this) {
          timerTaskEntry.next.prev = timerTaskEntry.prev
          timerTaskEntry.prev.next = timerTaskEntry.next
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          timerTaskEntry.list = null
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // 删除所有任务条目并将提供的功能应用于其中的每个条目
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      var head = root.next
      while (head ne root) {
        remove(head)
        f(head)
        head = root.next
      }
      expiration.set(-1L)
    }
  }

  // 返回与此对象相关的剩余延迟时间，以给定的时间单位表示
  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - Time.SYSTEM.hiResClockMs, 0), TimeUnit.MILLISECONDS)
  }

  // 按照过期时间进行比较 用于优先级队列
  def compareTo(d: Delayed): Int = {

    val other = d.asInstanceOf[TimerTaskList]

    if(getExpiration < other.getExpiration) -1
    else if(getExpiration > other.getExpiration) 1
    else 0
  }

}

// 定时任务器中存储的实体 {@see timerTask 要执行的定时任务}
private[timer] class TimerTaskEntry(val timerTask: TimerTask, val expirationMs: Long) extends Ordered[TimerTaskEntry] {

  // 相关实体节点
  @volatile
  var list: TimerTaskList = null
  var next: TimerTaskEntry = null
  var prev: TimerTaskEntry = null

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  if (timerTask != null) timerTask.setTimerTaskEntry(this)

  // 是否已经取消
  def cancelled: Boolean = {
    timerTask.getTimerTaskEntry != this
  }

  def remove(): Unit = {
    var currentList = list
    // 如果另一个线程将条目从任务条目列表移动到另一个线程时调用remove，则由于列表值的更改可能无法删除条目。
    // 因此，我们重试直到列表变为null。在极少数情况下，此线程会看到null并退出循环，但其他线程稍后会将该条目插入另一个列表。
    while (currentList != null) {
      currentList.remove(this)
      currentList = list
    }
  }

  override def compare(that: TimerTaskEntry): Int = {
    this.expirationMs compare that.expirationMs
  }
}

