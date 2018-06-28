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

import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * 向执行程序添加一个新任务。它将在任务延迟后执行。
    *（从提交的时间开始）
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}
// 系统级的定时器
@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // 执行任务的线程池
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      KafkaThread.nonDaemon("executor-"+executorName, runnable)
  })

  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  private[this] val taskCounter = new AtomicInteger(0) // 定时器中的任务数
  private[this] val timingWheel = new TimingWheel( // 时间轮转片
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      // 构建包含过期时间的TimerTaskEntry
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  // 添加任务实体
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    if (!timingWheel.add(timerTaskEntry)) { // 向时间轮添加任务失败，需要异步马上执行
      // 早已过期或者取消
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask) //  执行任务
    }
  }

  // 再次插入轮转
  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * 如果有一个过期的桶，则启动时钟。如果调用时没有任何过期的桶，则在放弃之前等待到时间超时。
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS) // 获取延时队列中过期时间最长桶
    if (bucket != null) {
      writeLock.lock()  // 写锁，禁止对时间轮修改
      try {
        while (bucket != null) {
          timingWheel.advanceClock(bucket.getExpiration())
          bucket.flush(reinsert) // 根据给定方法刷新桶内数据,用于添加到timingWheel，有可能执行任务
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown() {
    taskExecutor.shutdown()
  }

}

