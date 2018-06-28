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

package kafka.utils

import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.utils.Time

import java.util.concurrent.TimeUnit
import java.util.Random

import scala.math._

/**
 * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
 * (the units of the process don't matter, it could be bytes or a count of some other thing), and will sleep for
 * an appropriate amount of time when maybeThrottle() is called to attain the desired rate.
 * 一个测量和节制某些进程的速率的类。
  * 节流器每秒需要一个速率（进程的单元不重要，它可以是字节或其他事物的计数），
  * 并且当maybeThrottle()被调用以达到期望的速率时，它将休眠一段适当的时间。
 * @param desiredRatePerSec: 我们想以单位/秒命中率
 * @param checkIntervalMs: 检查我们的速率的间隔
 * @param throttleDown: 节流会增加还是降低我们的速度？
 * @param time: The time implementation to use
 */
@threadsafe
class Throttler(desiredRatePerSec: Double, // 每秒期望的速率
                checkIntervalMs: Long = 100L,
                throttleDown: Boolean = true,
                metricName: String = "throttler",
                units: String = "entries",
                time: Time = Time.SYSTEM) extends Logging with KafkaMetricsGroup {
  
  private val lock = new Object
  private val meter = newMeter(metricName, units, TimeUnit.SECONDS)
  private val checkIntervalNs = TimeUnit.MILLISECONDS.toNanos(checkIntervalMs)
  private var periodStartNs: Long = time.nanoseconds
  private var observedSoFar: Double = 0.0 // 到目前为止累加的观察值
  
  def maybeThrottle(observed: Double) {
    val msPerSec = TimeUnit.SECONDS.toMillis(1)// 1000
    val nsPerSec = TimeUnit.SECONDS.toNanos(1)// 1000000000

    meter.mark(observed.toLong)
    lock synchronized {
      observedSoFar += observed
      val now = time.nanoseconds
      val elapsedNs = now - periodStartNs
      // if we have completed an interval AND we have observed something, maybe
      // we should take a little nap
      if (elapsedNs > checkIntervalNs && observedSoFar > 0) {// 满足检查的条件
        val rateInSecs = (observedSoFar * nsPerSec) / elapsedNs
        val needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec))
        if (needAdjustment) {// 判断是否需要调整写入速率
          // solve for the amount of time to sleep to make us hit the desired rate
          val desiredRateMs = desiredRatePerSec / msPerSec.toDouble
          val elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNs)
          val sleepTime = round(observedSoFar / desiredRateMs - elapsedMs)
          if (sleepTime > 0) {
            trace("Natural rate is %f per second but desired rate is %f, sleeping for %d ms to compensate.".format(rateInSecs, desiredRatePerSec, sleepTime))
            time.sleep(sleepTime)
          }
        }
        periodStartNs = now
        observedSoFar = 0
      }
    }
  }

}

object Throttler {
  
  def main(args: Array[String]) {
    val nsPerSec = TimeUnit.SECONDS.toNanos(1)
    val msPerSec = TimeUnit.SECONDS.toMillis(1)
    val rand = new Random()
    val throttler = new Throttler(1000, 100, true, time = Time.SYSTEM)
    val interval = 30000
    var start = System.currentTimeMillis
    var total = 0
    while(true) {
      val value = rand.nextInt(1000)
      Thread.sleep(1)
      throttler.maybeThrottle(value)
      total += value
      val now = System.currentTimeMillis
      if(now - start >= interval) {
        println(total / (interval/1000.0))
        start = now
        total = 0
      }
    }
  }
}
