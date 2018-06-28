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

import java.util.Arrays
import java.security.MessageDigest
import java.nio.ByteBuffer
import kafka.utils._
import org.apache.kafka.common.utils.Utils

trait OffsetMap {
  def slots: Int
  def put(key: ByteBuffer, offset: Long)
  def get(key: ByteBuffer): Long
  def updateLatestOffset(offset: Long)
  def clear()
  def size: Int
  def utilization: Double = size.toDouble / slots
  def latestOffset: Long
}

/**
  * 用于删除日志的哈希表。该哈希表使用密钥的加密安全散列作为比较密钥的代理，并节省对象开销的空间。
  * 碰撞是通过探测来解决的。此哈希表不支持删除。（内存利用效率高）
 * @param memory The amount of memory this map can use
 * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512
 */
@nonthreadsafe
class SkimpyOffsetMap(val memory: Int, val hashAlgorithm: String = "MD5") extends OffsetMap {
  private val bytes = ByteBuffer.allocate(memory)
  
  /* the hash algorithm instance to use, default is MD5 */
  private val digest = MessageDigest.getInstance(hashAlgorithm)
  
  /* 哈希算法的字节数 */
  private val hashSize = digest.getDigestLength// MD5 16
  
  /* 创建一些散列缓冲区以避免每次重新分配 */
  private val hash1 = new Array[Byte](hashSize)
  private val hash2 = new Array[Byte](hashSize)
  
  /* number of entries put into the map */
  private var entries = 0
  
  /* number of lookups on the map */
  private var lookups = 0L
  
  /* the number of probes for all lookups */
  private var probes = 0L

  /* the latest offset written into the map */
  private var lastOffset = -1L

  /**
   * 每个条目使用的空间字节数（散列加上8字节偏移量的字节数  long）
   */
  val bytesPerEntry = hashSize + 8
  
  /**
   * The maximum number of entries this map can contain
   */
  val slots: Int = memory / bytesPerEntry
  
  /**
   * 将此偏移量与给定的键关联。
   * @param key The key
   * @param offset The offset
   */
  override def put(key: ByteBuffer, offset: Long) {
    require(entries < slots, "Attempt to add a new entry to a full offset map.")
    lookups += 1
    hashInto(key, hash1)
    // probe until we find the first empty slot
    var attempt = 0
    var pos = positionOf(hash1, attempt)  // 计算position
    while(!isEmpty(pos)) {
      // 不为空，进行尝试再次插入
      bytes.position(pos)
      bytes.get(hash2)
      if(Arrays.equals(hash1, hash2)) {// 发现相同的值，进行覆盖
        // we found an existing entry, overwrite it and return (size does not change)
        bytes.putLong(offset)
        lastOffset = offset
        return
      }
      attempt += 1
      pos = positionOf(hash1, attempt)
    }
    // found an empty slot, update it--size grows by 1
    // 进行插入
    bytes.position(pos)
    bytes.put(hash1)
    bytes.putLong(offset)
    lastOffset = offset
    entries += 1
  }
  
  /**
   * Check that there is no entry at the given position
   */
  private def isEmpty(position: Int): Boolean = 
    bytes.getLong(position) == 0 && bytes.getLong(position + 8) == 0 && bytes.getLong(position + 16) == 0

  /**
   * 获取与此键关联的偏移量
   * @param key The key
   * @return The offset associated with this key or -1 if the key is not found
   */
  override def get(key: ByteBuffer): Long = {
    lookups += 1
    hashInto(key, hash1)
    // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
    var attempt = 0
    var pos = 0
    //we need to guard against attempt integer overflow if the map is full
    //limit attempt to number of slots once positionOf(..) enters linear search mode
    val maxAttempts = slots + hashSize - 4
    do {
     if(attempt >= maxAttempts)
        return -1L
      pos = positionOf(hash1, attempt)
      bytes.position(pos)
      if(isEmpty(pos))
        return -1L
      bytes.get(hash2)
      attempt += 1
    } while(!Arrays.equals(hash1, hash2))// 相同则表示正确获取到键
    bytes.getLong()
  }
  
  /**
   * Change the salt used for key hashing making all existing keys unfindable.
   */
  override def clear() {
    this.entries = 0
    this.lookups = 0L
    this.probes = 0L
    this.lastOffset = -1L
    Arrays.fill(bytes.array, bytes.arrayOffset, bytes.arrayOffset + bytes.limit(), 0.toByte)
  }
  
  /**
   * The number of entries put into the map (note that not all may remain)
   */
  override def size: Int = entries
  
  /**
   * 查找中的碰撞速率
   */
  def collisionRate: Double = 
    (this.probes - this.lookups) / this.lookups.toDouble

  /**
   * The latest offset put into the map
   */
  override def latestOffset: Long = lastOffset

  override def updateLatestOffset(offset: Long): Unit = {
    lastOffset = offset
  }

  /**
    * 计算探头位置。我们首先尝试从散列本身读取连续的整数，然后如果所有这些失败，我们退化到线性探测。
   * @param hash The hash of the key to find the position for
   * @param attempt The ith probe
   * @return The byte offset in the buffer at which the ith probing for the given hash would reside
   */
  private def positionOf(hash: Array[Byte], attempt: Int): Int = {
    // 计算出探头
    val probe = CoreUtils.readInt(hash, math.min(attempt, hashSize - 4)) + math.max(0, attempt - hashSize + 4)
    val slot = Utils.abs(probe) % slots //计算出槽点
    this.probes += 1
    slot * bytesPerEntry
  }
  
  /**
   * 我们存储了给定密钥的偏移量。
   * @param key The key to hash
   * @param buffer The buffer to store the hash into
   */
  private def hashInto(key: ByteBuffer, buffer: Array[Byte]) {
    key.mark()
    digest.update(key)
    key.reset()
    digest.digest(buffer, 0, hashSize)
  }
  
}

object SkimpyOffsetMap{
  def main(args: Array[String]): Unit = {
    val digest = MessageDigest.getInstance("MD5")
    println(digest.getDigestLength)
  }
}
