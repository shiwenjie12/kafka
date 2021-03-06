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
package kafka.server.checkpoints

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import kafka.server.LogDirFailureChannel
import kafka.utils.Logging
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.utils.Utils

import scala.collection.{Seq, mutable}

trait CheckpointFileFormatter[T]{
  def toLine(entry: T): String

  def fromLine(line: String): Option[T]
}

// 将实体写入到检查点文件中
class CheckpointFile[T](val file: File,
                        version: Int,
                        formatter: CheckpointFileFormatter[T],// 实体的格式器
                        logDirFailureChannel: LogDirFailureChannel,
                        logDir: String) extends Logging {
  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()

  try Files.createFile(file.toPath) // create the file if it doesn't exist
  catch { case _: FileAlreadyExistsException => }

  // 向检查点文件写入实体
  def write(entries: Seq[T]) {
    lock synchronized {
      try {
        // 写入临时文件，然后与现有文件交换
        val fileOutputStream = new FileOutputStream(tempPath.toFile)
        val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))
        try {
          writer.write(version.toString)
          writer.newLine()

          writer.write(entries.size.toString)
          writer.newLine()

          entries.foreach { entry =>
            writer.write(formatter.toLine(entry))
            writer.newLine()
          }

          writer.flush()
          fileOutputStream.getFD().sync()
        } finally {
          writer.close()
        }

        Utils.atomicMoveWithFallback(tempPath, path)// 替换原始的文件
      } catch {
        case e: IOException =>
          val msg = s"Error while writing to checkpoint file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }

  // 从检查点文件中读取实体
  def read(): Seq[T] = {
    def malformedLineException(line: String) =
      new IOException(s"Malformed line in checkpoint file (${file.getAbsolutePath}): $line'")
    lock synchronized {
      try {
        val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))
        var line: String = null
        try {
          line = reader.readLine()
          if (line == null)
            return Seq.empty
          line.toInt match {
            case fileVersion if fileVersion == version =>
              line = reader.readLine()
              if (line == null)
                return Seq.empty
              val expectedSize = line.toInt // 期望的大小，写入时的大小
              val entries = mutable.Buffer[T]()
              line = reader.readLine()
              while (line != null) {
                val entry = formatter.fromLine(line)
                entry match {
                  case Some(e) =>
                    entries += e
                    line = reader.readLine()
                  case _ => throw malformedLineException(line)
                }
              }
              if (entries.size != expectedSize)
                throw new IOException(s"Expected $expectedSize entries in checkpoint file (${file.getAbsolutePath}), but found only ${entries.size}")
              entries
            case _ =>
              throw new IOException(s"Unrecognized version of the checkpoint file (${file.getAbsolutePath}): " + version)
          }
        } catch {
          case _: NumberFormatException => throw malformedLineException(line)
        } finally {
          reader.close()
        }
      } catch {
        case e: IOException =>
          val msg = s"Error while reading checkpoint file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }
}
