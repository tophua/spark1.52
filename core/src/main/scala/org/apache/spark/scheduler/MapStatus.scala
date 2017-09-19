/*
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

package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 * MapStatus 维护了map输出Block的地址BlockManagerId,所以reducer任务知道从何处获取map任务的中间输出
 * sealed被声明trait仅能被同一个文件的类继承
 */
private[spark] sealed trait MapStatus {
  /** 
   *  Location where this task was run. 
   *  运行Task任务的BlockManager的地址
   **/
 
  def location: BlockManagerId

  /**
   * Estimated size for the reduce block, in bytes.
   * 传给Reduce任务的Blok的估算大小
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
    * 如果块不为空,则该方法务必返回非零大小,这种不变性对于正确性是必需的,因为块抓取器被允许跳过零大小的块。
    *  task 输出的每个 FileSegment 大小
   */
  def getSizeForBlock(reduceId: Int): Long
}


private[spark] object MapStatus {

  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): MapStatus = {
    if (uncompressedSizes.length > 2000) {
      HighlyCompressedMapStatus(loc, uncompressedSizes)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes)
    }
  }

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
    * 压缩字节大小为8位，以便有效地报告map输出大小,我们通过将大小的日志基础1.1编码为整数来完成,可以支持高达35 GB的大小,最多10％的错误。
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
    * 使用compressSize的反向操作解压缩8位编码块大小
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 * 跟踪每个块的大小的实现,每个块的大小用单个字节表示
 * @param loc location where the task is being executed.执行任务的位置
 * @param compressedSizes size of the blocks, indexed by reduce partition id.块的大小,通过减少分区ID进行索引
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte])
  extends MapStatus with Externalizable {
  //仅用于反序列化
  protected def this() = this(null, null.asInstanceOf[Array[Byte]])  // For deserialization only

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long]) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize))
  }

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
  }
}

/**
 * A [[MapStatus]] implementation that only stores the average size of non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.  During serialization, this bitmap
 * is compressed.
  * 仅存储非空块的平均大小的实现,以及跟踪哪些块为空的位图。在序列化期间,该位图被压缩。
 *
 * @param loc location where the task is being executed执行任务的位置
 * @param numNonEmptyBlocks the number of non-empty blocks非空块的数量
 * @param emptyBlocks a bitmap tracking which blocks are empty一个位图跟踪哪些块是空的
 * @param avgSize average size of the non-empty blocks 非空块的平均大小
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long)
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  //在反序列化期间调用默认构造函数时,loc可以为null
  require(loc == null || avgSize > 0 || numNonEmptyBlocks == 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1)  // For deserialization only

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      avgSize
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.writeExternal(out)
    out.writeLong(avgSize)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.readExternal(in)
    avgSize = in.readLong()
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    //我们必须跟踪哪些块是空的,以便在使用平均块大小时,我们不会将零大小的块报告为非空（或反之亦然）
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var totalSize: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    //从压缩的角度来看,跟踪空块或非空块也不重要,从性能的角度来看,
    // 我们受益于跟踪空块,因为我们期望它们将少得多,因此我们将执行较少的位图插入。
    val emptyBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    while (i < totalNumBlocks) {
      var size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        totalSize += size
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }
    val avgSize = if (numNonEmptyBlocks > 0) {
      totalSize / numNonEmptyBlocks
    } else {
      0
    }
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize)
  }
}
