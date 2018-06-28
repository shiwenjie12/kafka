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

package kafka.log

import java.io.{File, IOException, RandomAccessFile}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.log.IndexSearchType.IndexSearchEntity
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{MappedByteBuffers, OperatingSystem, Utils}

import scala.math.ceil

/**
 * 持有索引格式的抽象索引类。
 *
 * @param file 索引文件
 * @param baseOffset 该索引对应的段的基偏移量。
 * @param maxIndexSize 以字节为单位的最大索引大小。
 */
abstract class AbstractIndex[K, V](@volatile var file: File, val baseOffset: Long,
                                   val maxIndexSize: Int = -1, val writable: Boolean) extends Logging {

  // Length of the index file
  @volatile
  private var _length: Long = _

  // 实体大小，文件内容长度以实体大小倍数
  protected def entrySize: Int

  protected val lock = new ReentrantLock

  @volatile
  protected var mmap: MappedByteBuffer = {
    val newlyCreated = file.createNewFile()
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")
    try {
      /* 如果是必须的，则预分配文件大小 */
      if(newlyCreated) {
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }

      /* memory-map the file */
      _length = raf.length()
      val idx = {
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }
      /* set the position in the index for the next entry */
      if(newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is valid and set position to last entry
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      CoreUtils.swallow(raf.close(), this)
    }
  }

  /**
   * The maximum number of entries this index can hold
   */
  @volatile
  private[this] var _maxEntries = mmap.limit() / entrySize

  /** The number of entries in this index */
  @volatile
  protected var _entries = mmap.position() / entrySize

  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = _entries >= _maxEntries

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = _length

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   *
   * @param newSize new size of the index file
   * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
   */
  def resize(newSize: Int): Boolean = {
    inLock(lock) {
      val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)

      if (_length == roundedNewSize) {
        false
      } else {
        val raf = new RandomAccessFile(file, "rw")
        try {
          val position = mmap.position()

          /* 在文件被映射时，Windows不会让我们修改文件长度。 :-( */
          if (OperatingSystem.IS_WINDOWS)
            safeForceUnmap()
          raf.setLength(roundedNewSize)
          _length = roundedNewSize
          mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
          _maxEntries = mmap.limit() / entrySize
          mmap.position(position)
          true
        } finally {
          CoreUtils.swallow(raf.close(), this)
        }
      }
    }
  }

  /**
   * Rename the file that backs this offset index
   *
   * @throws IOException if rename fails
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally file = f
  }

  /**
   * Flush the data in the index to disk
   */
  def flush() {
    inLock(lock) {
      mmap.force()
    }
  }

  /**
   * Delete this index file.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    inLock(lock) {
      // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.s
      // 在JVM上，内存映射通常是由垃圾回收器映射的。
      // 然而，在某些情况下，它可以暂停应用线程（STW）很长一段时间从物理磁盘读取元数据。
      // 为了防止这一点，我们强制清除内存映射在正确的执行，这从来没有影响API响应性。
      safeForceUnmap()
    }
    Files.deleteIfExists(file.toPath)
  }

  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize() {
    inLock(lock) {
      resize(entrySize * _entries)
    }
  }

  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes = entrySize * _entries

  /** Close the index */
  def close() {
    trimToValidSize()
  }

  def closeHandler(): Unit = {
    inLock(lock) {
      safeForceUnmap()
    }
  }

  /**
   * Do a basic sanity check on this index to detect obvious problems
   *
   * @throws CorruptIndexException if any problems are found
   */
  def sanityCheck(): Unit

  /**
   * Remove all the entries from the index.
   */
  protected def truncate(): Unit

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long): Unit

  /**
   * Remove all the entries from the index and resize the index to the max index size.
   */
  def reset(): Unit = {
    truncate()
    resize(maxIndexSize)
  }

  protected def safeForceUnmap(): Unit = {
    try forceUnmap()
    catch {
      case t: Throwable => error(s"Error unmapping index $file", t)
    }
  }

  /**
   * 强制释放缓冲区的MMAP。
   */
  protected[log] def forceUnmap() {
    try MappedByteBuffers.unmap(file.getAbsolutePath, mmap)
    finally mmap = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
  }

  /**
   * Execute the given function in a lock only if we are running on windows. We do this
   * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS)
        lock.unlock()
    }
  }

  /**
   * To parse an entry in the index.
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n the slot
   * @return the index entry stored in the given slot.
   */
  protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

  /**
   * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
   * The comparison is made using the `IndexEntry.compareTo()` method.
   *
   * @param idx The index buffer
   * @param target The index key to look for
   * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
   */
  protected def largestLowerBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._1

  /**
   * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
   */
  protected def smallestUpperBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._2

  /**
   * 查找给定目标的下界和上界。
   */
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): (Int, Int) = {
    // check if the index is empty
    if(_entries == 0)
      return (-1, -1)

    // check if the target offset is smaller than the least offset
    if(compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
      return (-1, 0)

    // binary search for the entry
    var lo = 0
    var hi = _entries - 1
    while(lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      val found = parseEntry(idx, mid)
      val compareResult = compareIndexEntry(found, target, searchEntity)
      if(compareResult > 0)
        hi = mid - 1
      else if(compareResult < 0)
        lo = mid
      else
        return (mid, mid)
    }

    (lo, if (lo == _entries - 1) -1 else lo + 1)
  }

  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchEntity): Int = {
    searchEntity match {
      case IndexSearchType.KEY => indexEntry.indexKey.compareTo(target)
      case IndexSearchType.VALUE => indexEntry.indexValue.compareTo(target)
    }
  }

  /**
   * 将数字舍入为给定因子的最大精确倍数，小于给定数字。
   * E.g. roundDownToExactMultiple(67, 8) == 64
   */
  private def roundDownToExactMultiple(number: Int, factor: Int) = factor * (number / factor)

}

object IndexSearchType extends Enumeration {
  type IndexSearchEntity = Value
  val KEY, VALUE = Value
}
