/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import java.util.concurrent.locks.ReadWriteLock

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{MeasurableStat, MetricConfig, Metrics, Sensor}

/**
  * 类集中了用于创建/访问传感器的逻辑。
  * 可以通过在传递的度量配置中包装配额来更新配额。
  *
  * 后面的参数被传递为它们只在实例化传感器时调用的方法。
  */
class SensorAccess(lock: ReadWriteLock, metrics: Metrics) {

  def getOrCreate(sensorName: String, expirationTime: Long,
                  metricName: => MetricName, config: => Option[MetricConfig], measure: => MeasurableStat): Sensor = {
    var sensor: Sensor = null

    /**
      * 获取读取锁以获取传感器。从多个线程调用getSensor是安全的。
      * 读锁允许线程单独创建一个传感器。创建传感器的线程
      * 将获取写入锁，并防止传感器在创建它们时被读取。
      * 只需检查传感器是否为空，而不需要读取读锁，就足够了。
      * 存在的传感器并不意味着它被完全初始化，也就是说，所有的度量可能都没有被添加。
      * 这个读锁等待，直到写入线程已经释放它的锁，即完全初始化传感器。
      * 在这一点上阅读是安全的。
      */
    lock.readLock().lock()
    try sensor = metrics.getSensor(sensorName)
    finally lock.readLock().unlock()

    /*
     * 如果传感器为NULL，尝试创建它，否则返回现有传感器。
     * 传感器可以为NULL，因此空校验。
     */
    if (sensor == null) {
      /* Acquire a write lock because the sensor may not have been created and we only want one thread to create it.
       * Note that multiple threads may acquire the write lock if they all see a null sensor initially
       * In this case, the writer checks the sensor after acquiring the lock again.
       * This is safe from Double Checked Locking because the references are read
       * after acquiring read locks and hence they cannot see a partially published reference
       */
      lock.writeLock().lock()
      try {
        // Set the var for both sensors in case another thread has won the race to acquire the write lock. This will
        // ensure that we initialise `ClientSensors` with non-null parameters.
        sensor = metrics.getSensor(sensorName)
        if (sensor == null) {
          sensor = metrics.sensor(sensorName, config.orNull, expirationTime)
          sensor.add(metricName, measure)
        }
      } finally {
        lock.writeLock().unlock()
      }
    }
    sensor
  }
}
