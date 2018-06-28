/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 传感器将连续的数值序列应用到一组相关度量中。
 * 例如，一个传感器在消息大小会记录一系列消息的大小使用{@link #record(double)}的API和维持一套指标对请求的大小如平均或最大。
 */
public final class Sensor {

    private final Metrics registry;
    private final String name;
    private final Sensor[] parents;
    private final List<Stat> stats;
    private final List<KafkaMetric> metrics;
    private final MetricConfig config;
    private final Time time;
    private volatile long lastRecordTime;
    private final long inactiveSensorExpirationTimeMs;

    /**
     * 记录等级（info、debug）
     */
    public enum RecordingLevel {
        INFO(0, "INFO"), DEBUG(1, "DEBUG");

        private static final RecordingLevel[] ID_TO_TYPE;
        private static final int MIN_RECORDING_LEVEL_KEY = 0;
        public static final int MAX_RECORDING_LEVEL_KEY;

        static {
            int maxRL = -1;
            for (RecordingLevel level : RecordingLevel.values()) {
                maxRL = Math.max(maxRL, level.id);
            }
            RecordingLevel[] idToName = new RecordingLevel[maxRL + 1];
            for (RecordingLevel level : RecordingLevel.values()) {
                idToName[level.id] = level;
            }
            ID_TO_TYPE = idToName;
            MAX_RECORDING_LEVEL_KEY = maxRL;
        }

        /** an english description of the api--this is for debugging and can change */
        public final String name;

        /** the permanent and immutable id of an API--this can't change ever */
        public final short id;

        RecordingLevel(int id, String name) {
            this.id = (short) id;
            this.name = name;
        }

        public static RecordingLevel forId(int id) {
            if (id < MIN_RECORDING_LEVEL_KEY || id > MAX_RECORDING_LEVEL_KEY)
                throw new IllegalArgumentException(String.format("Unexpected RecordLevel id `%d`, it should be between `%d` " +
                    "and `%d` (inclusive)", id, MIN_RECORDING_LEVEL_KEY, MAX_RECORDING_LEVEL_KEY));
            return ID_TO_TYPE[id];
        }

        /** Case insensitive lookup by protocol name */
        public static RecordingLevel forName(String name) {
            return RecordingLevel.valueOf(name.toUpperCase(Locale.ROOT));
        }

        /**
         * 判断是否应该记录
         * @param configId
         * @return
         */
        public boolean shouldRecord(final int configId) {
            return configId == DEBUG.id || configId == this.id;
        }

    }

    private final RecordingLevel recordingLevel;

    Sensor(Metrics registry, String name, Sensor[] parents, MetricConfig config, Time time,
           long inactiveSensorExpirationTimeSeconds, RecordingLevel recordingLevel) {
        super();
        this.registry = registry;
        this.name = Utils.notNull(name);
        this.parents = parents == null ? new Sensor[0] : parents;
        this.metrics = new ArrayList<>();
        this.stats = new ArrayList<>();
        this.config = config;
        this.time = time;
        this.inactiveSensorExpirationTimeMs = TimeUnit.MILLISECONDS.convert(inactiveSensorExpirationTimeSeconds, TimeUnit.SECONDS);
        this.lastRecordTime = time.milliseconds();
        this.recordingLevel = recordingLevel;
        checkForest(new HashSet<Sensor>());
    }

    /* Validate that this sensor doesn't end up referencing itself */
    private void checkForest(Set<Sensor> sensors) {
        if (!sensors.add(this))
            throw new IllegalArgumentException("Circular dependency in sensors: " + name() + " is its own parent.");
        for (Sensor parent : parents)
            parent.checkForest(sensors);
    }

    /**
     * The name this sensor is registered with. This name will be unique among all registered sensors.
     */
    public String name() {
        return this.name;
    }

    /**
     * Record an occurrence, this is just short-hand for {@link #record(double) record(1.0)}
     */
    public void record() {
        if (shouldRecord()) {
            record(1.0);
        }
    }

    /**
     * @return true if the sensor's record level indicates that the metric will be recorded, false otherwise
     */
    public boolean shouldRecord() {
        return this.recordingLevel.shouldRecord(config.recordLevel().id);
    }
    /**
     * Record a value with this sensor
     * @param value The value to record
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value) {
        if (shouldRecord()) {
            record(value, time.milliseconds());
        }
    }

    /**
     * Record a value at a known time. This method is slightly faster than {@link #record(double)} since it will reuse
     * the time stamp.
     * @param value The value we are recording
     * @param timeMs The current POSIX time in milliseconds
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value, long timeMs) {
        record(value, timeMs, true);
    }

    public void record(double value, long timeMs, boolean checkQuotas) {
        if (shouldRecord()) {
            this.lastRecordTime = timeMs;
            synchronized (this) {
                // increment all the stats
                for (Stat stat : this.stats)
                    stat.record(config, value, timeMs);
                if (checkQuotas)
                    checkQuotas(timeMs);
            }
            for (Sensor parent : parents)
                parent.record(value, timeMs, checkQuotas);
        }
    }

    /**
     * 检查我们是否违反了任何配置配额的指标的配额
     */
    public void checkQuotas() {
        checkQuotas(time.milliseconds());
    }

    public void checkQuotas(long timeMs) {
        for (KafkaMetric metric : this.metrics) {
            MetricConfig config = metric.config();
            if (config != null) {
                Quota quota = config.quota(); // 获取配额
                if (quota != null) {
                    double value = metric.measurableValue(timeMs); // 获取特定时间下的测量值
                    if (!quota.acceptable(value)) {// 判断是否符合范围
                        throw new QuotaViolationException(metric.metricName(), value,
                            quota.bound());
                    }
                }
            }
        }
    }

    /**
     * Register a compound statistic with this sensor with no config override
     * @param stat The stat to register
     * @return true if stat is added to sensor, false if sensor is expired
     */
    public boolean add(CompoundStat stat) {
        return add(stat, null);
    }

    /**
     * Register a compound statistic with this sensor which yields multiple measurable quantities (like a histogram)
     * @param stat The stat to register
     * @param config The configuration for this stat. If null then the stat will use the default configuration for this
     *        sensor.
     * @return true if stat is added to sensor, false if sensor is expired
     */
    public synchronized boolean add(CompoundStat stat, MetricConfig config) {
        if (hasExpired())
            return false;

        this.stats.add(Utils.notNull(stat));
        Object lock = new Object();
        for (NamedMeasurable m : stat.stats()) {
            KafkaMetric metric = new KafkaMetric(lock, m.name(), m.stat(), config == null ? this.config : config, time);
            this.registry.registerMetric(metric);
            this.metrics.add(metric);
        }
        return true;
    }

    /**
     * Register a metric with this sensor
     * @param metricName The name of the metric
     * @param stat The statistic to keep
     * @return true if metric is added to sensor, false if sensor is expired
     */
    public boolean add(MetricName metricName, MeasurableStat stat) {
        return add(metricName, stat, null);
    }

    /**
     * Register a metric with this sensor
     * @param metricName The name of the metric
     * @param stat The statistic to keep
     * @param config A special configuration for this metric. If null use the sensor default configuration.
     * @return true if metric is added to sensor, false if sensor is expired
     */
    public synchronized boolean add(MetricName metricName, MeasurableStat stat, MetricConfig config) {
        if (hasExpired())
            return false;

        KafkaMetric metric = new KafkaMetric(new Object(),
                                             Utils.notNull(metricName),
                                             Utils.notNull(stat),
                                             config == null ? this.config : config,
                                             time);
        this.registry.registerMetric(metric);
        this.metrics.add(metric);
        this.stats.add(stat);
        return true;
    }

    /**
     * Return true if the Sensor is eligible for removal due to inactivity.
     *        false otherwise
     */
    public boolean hasExpired() {
        return (time.milliseconds() - this.lastRecordTime) > this.inactiveSensorExpirationTimeMs;
    }

    synchronized List<KafkaMetric> metrics() {
        return Collections.unmodifiableList(this.metrics);
    }
}
