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

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock, ReentrantReadWriteLock}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
  * 一个操作的处理需要被延迟至多给定的delayMs。 例如，延迟生产操作可能正在等待指定数量的确认;
  * 或延迟获取操作可能正在等待给定数量的字节进行累加。
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 */
abstract class DelayedOperation(override val delayMs: Long,
    lockOpt: Option[Lock] = None) extends TimerTask with Logging {

  private val completed = new AtomicBoolean(false)
  // Visible for testing
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /*
   * 如果没有完成，则强制完成延迟操作。
   * 此功能可在何时触发
   *
   * 1. 该操作已在tryComplete()内被验证为可完成的。
   * 2. 操作已经过期，因此需要立即完成。
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      cancel() // 取消定时器
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * 检查延迟操作是否已完成
   */
  def isCompleted: Boolean = completed.get()

  /**
   * 当延迟操作过期并被迫完成时，请回电执行。
   */
  def onExpiration(): Unit

  /**
    * 完成一个操作的过程；这个函数需要在子类中定义，并且在forceComplete()中被精确调用一次。
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   */
  def tryComplete(): Boolean

  /**
    * 线程安全的tryComplete()的变体，只有在没有阻塞的情况下才能获取锁才能尝试完成。
   */
  private[server] def maybeTryComplete(): Boolean = {
    if (lock.tryLock()) {
      try {
        tryComplete()
      } finally {
        lock.unlock()
      }
    } else
      false
  }

  /*
   * run() method defines a task that is executed on timeout
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
  * 一个辅助净化类，用于簿记延迟超时操作，并过期超时操作。
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true, // 是否启用收割机
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

  private val removeWatchersLock = new ReentrantReadWriteLock()

  // 炼狱中估计的总操作数
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* 后台线程过期已过时的操作 */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value: Int = watched
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value: Int = delayed
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * 检查是否可以完成操作，如果不能，则根据给定的观察键监控它
    *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    // At this point the only thread that can attempt this operation is this current thread
    // Hence it is safe to tryComplete() without a lock
    // 自我完成延迟操作
    var isCompletedByMe = operation.tryComplete()
    if (isCompletedByMe)
      return true

    var watchCreated = false
    for(key <- watchKeys) { // 向每个监控键中添加延迟操作
      // 如果操作已经完成，请停止将其添加到观察者列表的其余部分。
      if (operation.isCompleted)
        return false
      watchForOperation(key, operation) // 对操作进行监控

      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }

    // 再次尝试完成延迟操作
    isCompletedByMe = operation.maybeTryComplete()
    if (isCompletedByMe)
      return true

    // 如果现在不能完成并因此需要被监视，则添加到过期队列中。
    if (!operation.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(operation)
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
    if(watchers == null)
      0
    else
      watchers.tryCompleteWatched()
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = allWatchers.map(_.countWatched).sum

  /**
   * 返回过期队列中的延迟操作数
   */
  def delayed: Int = timeoutTimer.size

  /**
    * 取消观看给定键的任何延迟操作。 注意操作将不会完成
    */
  def cancelForKey(key: Any): List[T] = {
    inWriteLock(removeWatchersLock) {
      val watchers = watchersForKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }
  /*
   * Return all the current watcher lists,
   * note that the returned watchers may be removed from the list by other threads
   */
  private def allWatchers = inReadLock(removeWatchersLock) { watchersForKey.values }

  /*
   * 返回给定键的监视列表，注意我们需要抓取removeWatchersLock，以避免将操作添加到删除的观察者列表中。
   */
  private def watchForOperation(key: Any, operation: T) {
    inReadLock(removeWatchersLock) {
      val watcher = watchersForKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * 如果列表为空，则从观察者列表中删除密钥。
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
    inWriteLock(removeWatchersLock) {
      // 如果当前键不再与观察者相关，则跳过
      if (watchersForKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        watchersForKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * 基于密钥的被监视延迟操作链表
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: T) {
      operations.add(t)
    }

    // 遍历列表并尝试完成一些监视元素，返回完成数
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // 另一个线程已完成此操作，只需删除它
          iter.remove()
        } else if (curr.maybeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    // 取消当前监控器的所有延时操作
    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // 遍历列表并清除已由他人完成的元素
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long) {
    timeoutTimer.advanceClock(timeoutMs) // 启动时钟

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    if (estimatedTotalOperations.get - delayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(delayed)
      debug("Begin purging watch lists")
      val purged = allWatchers.map(_.purgeCompleted()).sum
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * 后台收割到期超时的延迟操作
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork() {
      advanceClock(200L)
    }
  }
}
