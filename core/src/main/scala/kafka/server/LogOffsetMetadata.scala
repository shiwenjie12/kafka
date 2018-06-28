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

import org.apache.kafka.common.KafkaException

object LogOffsetMetadata {
  val UnknownOffsetMetadata = new LogOffsetMetadata(-1, 0, 0)
  val UnknownSegBaseOffset = -1L
  val UnknownFilePosition = -1

  class OffsetOrdering extends Ordering[LogOffsetMetadata] {
    override def compare(x: LogOffsetMetadata, y: LogOffsetMetadata): Int = {
      x.offsetDiff(y).toInt
    }
  }

}

/*
 * 日志偏移结构，包括：
 *  1. 消息偏移
 *  2. 定位段的基本消息偏移量
 *  3. 定位段的物理位置
 */
case class LogOffsetMetadata(messageOffset: Long,
                             segmentBaseOffset: Long = LogOffsetMetadata.UnknownSegBaseOffset,
                             relativePositionInSegment: Int = LogOffsetMetadata.UnknownFilePosition) {

  // 检查这个偏移量是否已经在比给定的偏移量更老的段上
  def onOlderSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly)
      throw new KafkaException(s"$this cannot compare its segment info with $that since it only has message offset info")

    this.segmentBaseOffset < that.segmentBaseOffset
  }

  // check if this offset is on the same segment with the given offset
  def onSameSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly)
      throw new KafkaException(s"$this cannot compare its segment info with $that since it only has message offset info")

    this.segmentBaseOffset == that.segmentBaseOffset
  }

  // 计算此偏移量与给定偏移量之间的消息数
  def offsetDiff(that: LogOffsetMetadata): Long = {
    this.messageOffset - that.messageOffset
  }

  // compute the number of bytes between this offset to the given offset
  // if they are on the same segment and this offset precedes the given offset
  def positionDiff(that: LogOffsetMetadata): Int = {
    if(!onSameSegment(that))
      throw new KafkaException(s"$this cannot compare its segment position with $that since they are not on the same segment")
    if(messageOffsetOnly)
      throw new KafkaException(s"$this cannot compare its segment position with $that since it only has message offset info")

    this.relativePositionInSegment - that.relativePositionInSegment
  }

  // decide if the offset metadata only contains message offset info
  def messageOffsetOnly: Boolean = {
    segmentBaseOffset == LogOffsetMetadata.UnknownSegBaseOffset && relativePositionInSegment == LogOffsetMetadata.UnknownFilePosition
  }

  override def toString = messageOffset.toString + " [" + segmentBaseOffset + " : " + relativePositionInSegment + "]"

}
