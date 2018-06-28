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

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils.inLock
import kafka.common.InvalidOffsetException

/**
  * 将偏移量映射到特定日志段的物理文件位置的索引。该索引可能是稀疏的：也就是说，它可能不为日志中的所有消息保留条目。
 *
 * 该索引存储在预先分配的文件中，以保存8字节条目的固定最大数量。
 *
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 *
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 *
 * The frequency of entries is up to the user of this class.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
// Avoid shadowing mutable `file` in AbstractIndex
class OffsetIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
    extends AbstractIndex[Long, Int](_file, baseOffset, maxIndexSize, writable) {

  override def entrySize = 8

  /* the last offset in the index */
  private[this] var _lastOffset = lastEntry.offset

  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
    .format(file.getAbsolutePath, maxEntries, maxIndexSize, _entries, _lastOffset, mmap.position()))

  /**
    * 索引中最后一个实体
   */
  private def lastEntry: OffsetPosition = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0)
        case s => parseEntry(mmap, s - 1).asInstanceOf[OffsetPosition]
      }
    }
  }

  def lastOffset: Long = _lastOffset

  /**
    * 找到小于或等于给定targetOffset的最大偏移量
    * 并返回一个包含该偏移量及其相应物理文件位置的对。
   *
   * @param targetOffset The offset to look up.
   * @return The offset found and the corresponding file position for this offset
   *         If the target offset is smaller than the least entry in the index (or the index is empty),
   *         the pair (baseOffset, 0) is returned.
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY)
      if(slot == -1)
        OffsetPosition(baseOffset, 0)
      else
        parseEntry(idx, slot).asInstanceOf[OffsetPosition]
    }
  }

  /**
   * Find an upper bound offset for the given fetch starting position and size. This is an offset which
   * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
   * such offset.
   */
  def fetchUpperBoundOffset(fetchOffset: OffsetPosition, fetchSize: Int): Option[OffsetPosition] = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = smallestUpperBoundSlotFor(idx, fetchOffset.position + fetchSize, IndexSearchType.VALUE)
      if (slot == -1)
        None
      else
        Some(parseEntry(idx, slot).asInstanceOf[OffsetPosition])
    }
  }

  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize)

  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 4)

  /**
    * 实解析实体
    * @param buffer 此内存映射索引的缓冲区。
    * @param n 插槽 即  第n个实体
    * @return the index entry stored in the given slot.
    */
  override def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry = {
      OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
  }

  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n, _entries))
      val idx = mmap.duplicate
      OffsetPosition(relativeOffset(idx, n), physical(idx, n))
    }
  }

  /**
   * 将给定偏移/位置对的条目追加到索引中。
    * 该条目必须具有比所有后续条目更大的偏移量。
   */
  def append(offset: Long, position: Int) {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      if (_entries == 0 || offset > _lastOffset) {
        debug("Adding index entry %d => %d to %s.".format(offset, position, file.getName))
        mmap.putInt((offset - baseOffset).toInt)
        mmap.putInt(position)
        _entries += 1
        _lastOffset = offset
        require(_entries * entrySize == mmap.position(), entries + " entries but file position in index is " + mmap.position() + ".")
      } else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
          .format(offset, entries, _lastOffset, file.getAbsolutePath))
      }
    }
  }

  override def truncate() = truncateToEntries(0)

  override def truncateTo(offset: Long) {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY)// 获取槽点

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
    * 截取索引待一个已知的实体
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastOffset = lastEntry.offset
    }
  }

  override def sanityCheck() {
    if (_entries != 0 && _lastOffset <= baseOffset)
      throw new CorruptIndexException(s"Corrupt index found, index file (${file.getAbsolutePath}) has non-zero size " +
        s"but the last offset is ${_lastOffset} which is no greater than the base offset $baseOffset.")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Index file ${file.getAbsolutePath} is corrupt, found $length bytes which is " +
        s"neither positive nor a multiple of $entrySize.")
  }

}
