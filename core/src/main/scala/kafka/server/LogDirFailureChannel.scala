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

import java.io.IOException
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import kafka.utils.Logging

/*
 * LogDirFailureChannel 通道允许外部线程阻止等待新的脱机日志目录。
 *
 * There should be a single instance of LogDirFailureChannel accessible by any class that does disk-IO operation.
 * If IOException is encountered while accessing a log directory, the corresponding class can add the log directory name
 * to the LogDirFailureChannel using maybeAddOfflineLogDir(). Each log directory will be added only once. After a log
 * directory is added for the first time, a thread which is blocked waiting for new offline log directories
 * can take the name of the new offline log directory out of the LogDirFailureChannel and handle the log failure properly.
 * An offline log directory will stay offline until the broker is restarted.
 *
 */
class LogDirFailureChannel(logDirNum: Int) extends Logging {

  private val offlineLogDirs = new ConcurrentHashMap[String, String]
  private val offlineLogDirQueue = new ArrayBlockingQueue[String](logDirNum)

  /*
   * If the given logDir is not already offline, add it to the
   * set of offline log dirs and enqueue it to the logDirFailureEvent queue
   * 添加脱机日志
   */
  def maybeAddOfflineLogDir(logDir: String, msg: => String, e: IOException): Unit = {
    error(msg, e)
    if (offlineLogDirs.putIfAbsent(logDir, logDir) == null) { // 保证添加的唯一性
      offlineLogDirQueue.add(logDir)
    }
  }

  /*
   * 从logDirFailureEvent队列获取下一个脱机日志目录。
   * 如果需要的话，该方法将等待，直到新的脱机日志目录可用。
   */
  def takeNextOfflineLogDir(): String = offlineLogDirQueue.take()

}
