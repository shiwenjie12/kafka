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

import java.util.concurrent.{ConcurrentHashMap, DelayQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.{Logging, ShutdownableThread}
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Avg, Rate, Total}
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.collection.JavaConverters._

/**
 * 表示每个客户端聚集的传感器。
 * @param quotaEntity Quota entity representing <client-id>, <user> or <user, client-id>
 * @param quotaSensor @跟踪配额的传感器
 * @param throttleTimeSensor @跟踪节气门时间的传感器
 */
case class ClientSensors(quotaEntity: QuotaEntity, quotaSensor: Sensor, throttleTimeSensor: Sensor)

/**
 * 配额管理的配置设置
 * @param quotaBytesPerSecondDefault 如果没有设置动态缺省值或用户配额，则分配给任何客户端ID的默认字节每秒配额。
 * @param numQuotaSamples 内存中保留的样本数
 * @param quotaWindowSizeSeconds 每个样本的时间跨度
 *
 */
case class ClientQuotaManagerConfig(quotaBytesPerSecondDefault: Long =
                                        ClientQuotaManagerConfig.QuotaBytesPerSecondDefault,
                                    numQuotaSamples: Int =
                                        ClientQuotaManagerConfig.DefaultNumQuotaSamples,
                                    quotaWindowSizeSeconds: Int =
                                        ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds)

object ClientQuotaManagerConfig {
  val QuotaBytesPerSecondDefault = Long.MaxValue
  // Always have 10 whole windows + 1 current window
  val DefaultNumQuotaSamples = 11
  val DefaultQuotaWindowSizeSeconds = 1
  // Purge sensors after 1 hour of inactivity
  val InactiveSensorExpirationTimeSeconds  = 3600
  val QuotaRequestPercentDefault = Int.MaxValue.toDouble
  val NanosToPercentagePerSecond = 100.0 / TimeUnit.SECONDS.toNanos(1)

  // 无限制的配额
  val UnlimitedQuota = Quota.upperBound(Long.MaxValue)
  // 默认的配额id
  val DefaultClientIdQuotaId = QuotaId(None, Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
  val DefaultUserQuotaId = QuotaId(Some(ConfigEntityName.Default), None, None)
  val DefaultUserClientIdQuotaId = QuotaId(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
}
// 限制条件
object QuotaTypes {
  val NoQuotas = 0
  val ClientIdQuotaEnabled = 1
  val UserQuotaEnabled = 2
  val UserClientIdQuotaEnabled = 4
}

// 配额Id
case class QuotaId(sanitizedUser: Option[String], clientId: Option[String], sanitizedClientId: Option[String])
// 配额实体
case class QuotaEntity(quotaId: QuotaId, sanitizedUser: String, clientId: String, sanitizedClientId: String, quota: Quota)

/**
  * 每个客户端度量记录的助手类。它还负责维护所有客户的配额使用统计。
 * <p/>
 * Quotas can be set at <user, client-id>, user or client-id levels. For a given client connection,
 * the most specific quota matching the connection will be applied. For example, if both a <user, client-id>
 * and a user quota match a connection, the <user, client-id> quota will be used. Otherwise, user quota takes
 * precedence over client-id quota. The order of precedence is:
 * <ul>
 *   <li>/config/users/<user>/clients/<client-id>
 *   <li>/config/users/<user>/clients/<default>
 *   <li>/config/users/<user>
 *   <li>/config/users/<default>/clients/<client-id>
 *   <li>/config/users/<default>/clients/<default>
 *   <li>/config/users/<default>
 *   <li>/config/clients/<client-id>
 *   <li>/config/clients/<default>
 * </ul>
  * 包括默认值在内的配额限制可以动态更新。对于配置了单一级别配额的情况，实现了优化。
 *
 * @param config @ClientQuotaManagerConfig quota configs
 * @param metrics @Metrics Metrics instance
 * @param quotaType Quota type of this quota manager
 * @param time @Time object to use
 */
class ClientQuotaManager(private val config: ClientQuotaManagerConfig,
                         private val metrics: Metrics,
                         private val quotaType: QuotaType,
                         private val time: Time,
                         threadNamePrefix: String) extends Logging {
  // 重载的配额
  private val overriddenQuota = new ConcurrentHashMap[QuotaId, Quota]()
  private val staticConfigClientIdQuota = Quota.upperBound(config.quotaBytesPerSecondDefault)
  @volatile private var quotaTypesEnabled = // 启用的配额格式
    if (config.quotaBytesPerSecondDefault == Long.MaxValue) QuotaTypes.NoQuotas
    else QuotaTypes.ClientIdQuotaEnabled
  private val lock = new ReentrantReadWriteLock()
  private val delayQueue = new DelayQueue[ThrottledResponse]()
  private val sensorAccessor = new SensorAccess(lock, metrics)
  private[server] val throttledRequestReaper = new ThrottledRequestReaper(delayQueue, threadNamePrefix)

  private val delayQueueSensor = metrics.sensor(quotaType + "-delayQueue")
  delayQueueSensor.add(metrics.metricName("queue-size",
                                      quotaType.toString,
                                      "Tracks the size of the delay queue"), new Total())
  start() // Use start method to keep findbugs happy
  private def start() {
    throttledRequestReaper.start()
  }

  /**
   * 触发所有节流请求回调的收割机线程
   * @param delayQueue DelayQueue to dequeue from
   */
  class ThrottledRequestReaper(delayQueue: DelayQueue[ThrottledResponse], prefix: String) extends ShutdownableThread(
    s"${prefix}ThrottledRequestReaper-${quotaType}", false) {

    override def doWork(): Unit = {
      val response: ThrottledResponse = delayQueue.poll(1, TimeUnit.SECONDS)
      if (response != null) {
        // 在延迟队列大小的减量
        delayQueueSensor.record(-1)
        trace("Response throttled for: " + response.throttleTimeMs + " ms")
        response.execute()
      }
    }
  }

  /**
   * Returns true if any quotas are enabled for this quota manager. This is used
   * to determine if quota related metrics should be created.
   * Note: If any quotas (static defaults, dynamic defaults or quota overrides) have
   * been configured for this broker at any time for this quota type, quotasEnabled will
   * return true until the next broker restart, even if all quotas are subsequently deleted.
   */
  def quotasEnabled: Boolean = quotaTypesEnabled != QuotaTypes.NoQuotas

  /**
    * 记录user/clientId 改变一些度量节流（produced/consumed字节，请求处理时间等）。
    * 如果违反了配额，则在延迟之后调用回调，否则立即调用回调。
    * 节流时间计算可以由子类重写。
   * @param sanitizedUser 客户端使用的principal
   * @param clientId clientId that produced/fetched the data
   * @param value amount of data in bytes or request processing time as a percentage
   * @param callback callback函数。这将触发立即如果配额是不violated。
    *                 如果有一个配额违反出资的，这callback将被触发后的延时
   * @return Number of milliseconds to delay the response in case of Quota violation.
   *         Zero otherwise
   */
  def maybeRecordAndThrottle(sanitizedUser: String, clientId: String, value: Double, callback: Int => Unit): Int = {
    if (quotasEnabled) {
      val clientSensors = getOrCreateQuotaSensors(sanitizedUser, clientId)
      recordAndThrottleOnQuotaViolation(clientSensors, value, callback)
    } else {
      // Don't record any metrics if quotas are not enabled at any level
      val throttleTimeMs = 0
      callback(throttleTimeMs)
      throttleTimeMs
    }
  }

  def recordAndThrottleOnQuotaViolation(clientSensors: ClientSensors, value: Double, callback: Int => Unit): Int = {
    var throttleTimeMs = 0
    try {
      clientSensors.quotaSensor.record(value)
      // 如果配额未被违反，立即触发回调
      callback(0)
    } catch {
      case _: QuotaViolationException =>
        // 计算延迟
        val clientQuotaEntity = clientSensors.quotaEntity
        val clientMetric = metrics.metrics().get(clientRateMetricName(clientQuotaEntity.sanitizedUser, clientQuotaEntity.clientId))
        throttleTimeMs = throttleTime(clientMetric, getQuotaMetricConfig(clientQuotaEntity.quota)).toInt
        clientSensors.throttleTimeSensor.record(throttleTimeMs)
        // 将元素添加到延迟队列中
        delayQueue.add(new ThrottledResponse(time, throttleTimeMs, callback))
        delayQueueSensor.record()
        debug("Quota violated for sensor (%s). Delay time: (%d)".format(clientSensors.quotaSensor.name(), throttleTimeMs))
    }
    throttleTimeMs
  }

  /**
   * Records that a user/clientId changed some metric being throttled without checking for
   * quota violation. The aggregate value will subsequently be used for throttling when the
   * next request is processed.
   */
  def recordNoThrottle(clientSensors: ClientSensors, value: Double) {
    clientSensors.quotaSensor.record(value, time.milliseconds(), false)
  }

  /**
   * Determines the quota-id for the client with the specified user principal
   * and client-id and returns the quota entity that encapsulates the quota-id
   * and the associated quota override or default quota.
   * 确定具有指定用户主体和客户机ID的客户机的配额标识，
    * 并返回封装配额标识和关联配额覆盖或默认配额的配额实体。
   */
  private def quotaEntity(sanitizedUser: String, clientId: String, sanitizedClientId: String) : QuotaEntity = {
    quotaTypesEnabled match { // 按分类进行创建
      case QuotaTypes.NoQuotas | QuotaTypes.ClientIdQuotaEnabled =>
        val quotaId = QuotaId(None, Some(clientId), Some(sanitizedClientId))
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultClientIdQuotaId)
          if (quota == null)
            quota = staticConfigClientIdQuota
        }
        QuotaEntity(quotaId, "", clientId, sanitizedClientId, quota)
      case QuotaTypes.UserQuotaEnabled =>
        val quotaId = QuotaId(Some(sanitizedUser), None, None)
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultUserQuotaId)
          if (quota == null)
            quota = ClientQuotaManagerConfig.UnlimitedQuota
        }
        QuotaEntity(quotaId, sanitizedUser, "", "", quota)
      case QuotaTypes.UserClientIdQuotaEnabled =>
        val quotaId = QuotaId(Some(sanitizedUser), Some(clientId), Some(sanitizedClientId))
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(QuotaId(Some(sanitizedUser), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default)))
          if (quota == null) {
            quota = overriddenQuota.get(QuotaId(Some(ConfigEntityName.Default), Some(clientId), Some(sanitizedClientId)))
            if (quota == null) {
              quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultUserClientIdQuotaId)
              if (quota == null)
                quota = ClientQuotaManagerConfig.UnlimitedQuota
            }
          }
        }
        QuotaEntity(quotaId, sanitizedUser, clientId, sanitizedClientId, quota)
      case _ =>
        quotaEntityWithMultipleQuotaLevels(sanitizedUser, clientId, sanitizedClientId)
    }
  }

  private def quotaEntityWithMultipleQuotaLevels(sanitizedUser: String, clientId: String, sanitizerClientId: String) : QuotaEntity = {
    val userClientQuotaId = QuotaId(Some(sanitizedUser), Some(clientId), Some(sanitizerClientId))

    val userQuotaId = QuotaId(Some(sanitizedUser), None, None)
    val clientQuotaId = QuotaId(None, Some(clientId), Some(sanitizerClientId))
    var quotaId = userClientQuotaId
    var quotaConfigId = userClientQuotaId
    // 1) /config/users/<user>/clients/<client-id>
    var quota = overriddenQuota.get(quotaConfigId)
    if (quota == null) {
      // 2) /config/users/<user>/clients/<default>
      quotaId = userClientQuotaId
      quotaConfigId = QuotaId(Some(sanitizedUser), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
      quota = overriddenQuota.get(quotaConfigId)

      if (quota == null) {
        // 3) /config/users/<user>
        quotaId = userQuotaId
        quotaConfigId = quotaId
        quota = overriddenQuota.get(quotaConfigId)

        if (quota == null) {
          // 4) /config/users/<default>/clients/<client-id>
          quotaId = userClientQuotaId
          quotaConfigId = QuotaId(Some(ConfigEntityName.Default), Some(clientId), Some(sanitizerClientId))
          quota = overriddenQuota.get(quotaConfigId)

          if (quota == null) {
            // 5) /config/users/<default>/clients/<default>
            quotaId = userClientQuotaId
            quotaConfigId = QuotaId(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
            quota = overriddenQuota.get(quotaConfigId)

            if (quota == null) {
              // 6) /config/users/<default>
              quotaId = userQuotaId
              quotaConfigId = QuotaId(Some(ConfigEntityName.Default), None, None)
              quota = overriddenQuota.get(quotaConfigId)

              if (quota == null) {
                // 7) /config/clients/<client-id>
                quotaId = clientQuotaId
                quotaConfigId = QuotaId(None, Some(clientId), Some(sanitizerClientId))
                quota = overriddenQuota.get(quotaConfigId)

                if (quota == null) {
                  // 8) /config/clients/<default>
                  quotaId = clientQuotaId
                  quotaConfigId = QuotaId(None, Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
                  quota = overriddenQuota.get(quotaConfigId)

                  if (quota == null) {
                    quotaId = clientQuotaId
                    quotaConfigId = null
                    quota = staticConfigClientIdQuota
                  }
                }
              }
            }
          }
        }
      }
    }
    val quotaUser = if (quotaId == clientQuotaId) "" else sanitizedUser
    val quotaClientId = if (quotaId == userQuotaId) "" else clientId
    QuotaEntity(quotaId, quotaUser, quotaClientId, sanitizerClientId, quota)
  }

  /**
   * Returns the quota for the client with the specified (non-encoded) user principal and client-id.
   * 
   * Note: 这种方法很昂贵，只供测试用。
   */
  def quota(user: String, clientId: String) = {
    quotaEntity(Sanitizer.sanitize(user), clientId, Sanitizer.sanitize(clientId)).quota
  }

  /*
   * This calculates the amount of time needed to bring the metric within quota
   * assuming that no new metrics are recorded.
   *
   * Basically, if O is the observed rate and T is the target rate over a window of W, to bring O down to T,
   * we need to add a delay of X to W such that O * W / (W + X) = T.
   * Solving for X, we get X = (O - T)/T * W.
   */
  protected def throttleTime(clientMetric: KafkaMetric, config: MetricConfig): Long = {
    val rateMetric: Rate = measurableAsRate(clientMetric.metricName(), clientMetric.measurable())
    val quota = config.quota()
    val difference = clientMetric.value() - quota.bound
    // Use the precise window used by the rate calculation
    val throttleTimeMs = difference / quota.bound * rateMetric.windowSize(config, time.milliseconds())
    throttleTimeMs.round
  }

  // Casting to Rate because we only use Rate in Quota computation
  private def measurableAsRate(name: MetricName, measurable: Measurable): Rate = {
    measurable match {
      case r: Rate => r
      case _ => throw new IllegalArgumentException(s"Metric $name is not a Rate metric, value $measurable")
    }
  }

  /*
   * 这个函数或者为给定的客户端ID返回传感器或者创建它们，如果它们不存在，
   * 元组的第一个传感器就是配额强制传感器。第二个是节流时间传感器。
   */
  def getOrCreateQuotaSensors(sanitizedUser: String, clientId: String): ClientSensors = {
    val sanitizedClientId = Sanitizer.sanitize(clientId) // 将敏感的id进行净化
    val clientQuotaEntity = quotaEntity(sanitizedUser, clientId, sanitizedClientId)
    // Names of the sensors to access
    ClientSensors(
      clientQuotaEntity,
      sensorAccessor.getOrCreate(
        getQuotaSensorName(clientQuotaEntity.quotaId),
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        clientRateMetricName(clientQuotaEntity.sanitizedUser, clientQuotaEntity.clientId),
        Some(getQuotaMetricConfig(clientQuotaEntity.quota)),
        new Rate
      ),
      sensorAccessor.getOrCreate(
        getThrottleTimeSensorName(clientQuotaEntity.quotaId),
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        throttleMetricName(clientQuotaEntity),
        None,
        new Avg
      )
    )
  }

  private def getThrottleTimeSensorName(quotaId: QuotaId): String = quotaType + "ThrottleTime-" + quotaId.sanitizedUser.getOrElse("") + ':' + quotaId.clientId.getOrElse("")

  private def getQuotaSensorName(quotaId: QuotaId): String = quotaType + "-" + quotaId.sanitizedUser.getOrElse("") + ':' + quotaId.clientId.getOrElse("")

  protected def getQuotaMetricConfig(quota: Quota): MetricConfig = {
    new MetricConfig()
            .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
            .samples(config.numQuotaSamples)
            .quota(quota)
  }

  protected def getOrCreateSensor(sensorName: String, metricName: MetricName): Sensor = {
    sensorAccessor.getOrCreate(
        sensorName,
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        metricName,
        None,
        new Rate
      )
  }

  /**
    * 重写<user>、<client-id>或<user, client-id>或这些级别中任何一个的动态默认值。
   * @param sanitizedUser user to override if quota applies to <user> or <user, client-id>
   * @param clientId client to override if quota applies to <client-id> or <user, client-id>
   * @param sanitizedClientId sanitized client ID to override if quota applies to <client-id> or <user, client-id>
   * @param quota 如果配额重载则移除，否则应用自定义配额
   */
  def updateQuota(sanitizedUser: Option[String], clientId: Option[String], sanitizedClientId: Option[String], quota: Option[Quota]) {
    /*
     * Acquire the write lock to apply changes in the quota objects.
     * This method changes the quota in the overriddenQuota map and applies the update on the actual KafkaMetric object (if it exists).
     * If the KafkaMetric hasn't been created, the most recent value will be used from the overriddenQuota map.
     * The write lock prevents quota update and creation at the same time. It also guards against concurrent quota change
     * notifications
     */
    lock.writeLock().lock()
    try {
      val quotaId = QuotaId(sanitizedUser, clientId, sanitizedClientId) // 配额Id
      val userInfo = sanitizedUser match { // 用户信息
        case Some(ConfigEntityName.Default) => "default user "
        case Some(user) => "user " + user + " "
        case None => ""
      }
      val clientIdInfo = clientId match { // 客户信息
        case Some(ConfigEntityName.Default) => "default client-id"
        case Some(id) => "client-id " + id
        case None => ""
      }
      quota match {
        case Some(newQuota) =>
          info(s"Changing ${quotaType} quota for ${userInfo}${clientIdInfo} to $newQuota.bound}")
          overriddenQuota.put(quotaId, newQuota)// 重载的配置
          (sanitizedUser, clientId) match { // 更改配额种类
            case (Some(_), Some(_)) => quotaTypesEnabled |= QuotaTypes.UserClientIdQuotaEnabled
            case (Some(_), None) => quotaTypesEnabled |= QuotaTypes.UserQuotaEnabled
            case (None, Some(_)) => quotaTypesEnabled |= QuotaTypes.ClientIdQuotaEnabled
            case (None, None) =>
          }
        case None => // 如果没有配额，则移除指定配额id的配额
          info(s"Removing ${quotaType} quota for ${userInfo}${clientIdInfo}")
          overriddenQuota.remove(quotaId)
      }

      val quotaMetricName = clientRateMetricName(sanitizedUser.getOrElse(""), clientId.getOrElse(""))
      val allMetrics = metrics.metrics()

      // 如果定义了多个级别的配额，或者如果这是默认配额更新，则遍历度量以查找所有受影响的值。否则，只更新单一匹配的一个。
      val singleUpdate = quotaTypesEnabled match { // 自定义才会单更新
        case QuotaTypes.NoQuotas | QuotaTypes.ClientIdQuotaEnabled | QuotaTypes.UserQuotaEnabled | QuotaTypes.UserClientIdQuotaEnabled =>
          !sanitizedUser.filter(_ == ConfigEntityName.Default).isDefined && !clientId.filter(_ == ConfigEntityName.Default).isDefined
        case _ => false
      }
      // 根据条件获取指定的quotaEntity，然后再更新度量配额
      if (singleUpdate) {
          // Change the underlying metric config if the sensor has been created
          val metric = allMetrics.get(quotaMetricName)
          if (metric != null) {
            val metricConfigEntity = quotaEntity(sanitizedUser.getOrElse(""), clientId.getOrElse(""), sanitizedClientId.getOrElse(""))
            val newQuota = metricConfigEntity.quota
            info(s"Sensor for ${userInfo}${clientIdInfo} already exists. Changing quota to ${newQuota.bound()} in MetricConfig")
            metric.config(getQuotaMetricConfig(newQuota))
          }
      } else {
          allMetrics.asScala.filterKeys(n => n.name == quotaMetricName.name && n.group == quotaMetricName.group).foreach {
            case (metricName, metric) =>
              val userTag = if (metricName.tags.containsKey("user")) metricName.tags.get("user") else ""
              val clientIdTag = if (metricName.tags.containsKey("client-id")) metricName.tags.get("client-id") else ""
              val metricConfigEntity = quotaEntity(userTag, clientIdTag, Sanitizer.sanitize(clientIdTag))
              if (metricConfigEntity.quota != metric.config.quota) {
                val newQuota = metricConfigEntity.quota
                info(s"Sensor for quota-id ${metricConfigEntity.quotaId} already exists. Setting quota to ${newQuota.bound} in MetricConfig")
                metric.config(getQuotaMetricConfig(newQuota))
              }
          }
      }

    } finally {
      lock.writeLock().unlock()
    }
  }

  protected def clientRateMetricName(sanitizedUser: String, clientId: String): MetricName = {
    metrics.metricName("byte-rate", quotaType.toString,
                   "Tracking byte-rate per user/client-id",
                   "user", sanitizedUser,
                   "client-id", clientId)
  }

  private def throttleMetricName(quotaEntity: QuotaEntity): MetricName = {
    metrics.metricName("throttle-time",
                       quotaType.toString,
                       "Tracking average throttle-time per user/client-id",
                       "user", quotaEntity.sanitizedUser,
                       "client-id", quotaEntity.clientId)
  }

  def shutdown() = {
    throttledRequestReaper.shutdown()
  }
}
