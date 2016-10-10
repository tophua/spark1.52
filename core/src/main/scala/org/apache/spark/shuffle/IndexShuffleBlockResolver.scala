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

package org.apache.spark.shuffle

import java.io._

import com.google.common.io.ByteStreams

import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkEnv}

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 * 创建和维护Shuffle块文件和物理文件之间映射,一个shuffle数据块映射到单个文件
 * 块索引Shuffle管理器,通常用于获取Block索引文件,并根据索引文件读取Block文件,
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  private val transportConf = SparkTransportConf.fromSparkConf(conf)
  /**获取Shuffle数据索引文件,根据shuffleId和mapId**/
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }
  /**获取Shuffle数据索引文件,根据shuffleId和mapId**/
  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   * 从一个Map任务中删除,包含输出的数据文件和索引文件
   * */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      file.delete()
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      file.delete()
    }
  }

  /**
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   * 检查给定的索引文件和数据文件是否相互匹配,如果是的话,返回数据文件中的分区长度,否则返回空。
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // the index file should have `block + 1` longs as offset.
    if (index.length() != (blocks + 1) * 8) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    // 读取块的长度
    val in = try {
      new DataInputStream(new BufferedInputStream(new FileInputStream(index)))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      // 将偏移量转换成每一个块的长度
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file
    //数据文件的大小应与索引文件相匹配
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   * 用于在Block索引文件中记录各个Partition的偏移量信息,便于下游Stage的任务读取
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   * 
   *  */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = Utils.tempFileWith(indexFile)
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
    Utils.tryWithSafeFinally {
      // We take in lengths of each block, need to convert it to offsets.
      var offset = 0L
      out.writeLong(offset)
      for (length <- lengths) {
        offset += length
        out.writeLong(offset)
      }
    } {
      out.close()
    }

    val dataFile = getDataFile(shuffleId, mapId)
    // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
    // the following check and rename are atomic.
    synchronized {
      val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
      if (existingLengths != null) {
        // Another attempt for the same task has already written our map outputs successfully,
        // so just use the existing partition lengths and delete our temporary map outputs.
        System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
        if (dataTmp != null && dataTmp.exists()) {
          dataTmp.delete()
        }
        indexTmp.delete()
      } else {
        // This is the first successful attempt in writing the map outputs for this task,
        // so override any existing index and data files with the ones we wrote.
        //这是第一次成功尝试编写此任务的Map输出
        if (indexFile.exists()) {
          indexFile.delete()
        }
        if (dataFile.exists()) {
          dataFile.delete()
        }
        if (!indexTmp.renameTo(indexFile)) {
          throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
        }
        if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
          throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
        }
      }
    }
  }
  /**
   * 根据ShuffleBlockId读取索引文件
   */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    //根据shuffleId,mapId获取索引文件
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      ByteStreams.skipFully(in, blockId.reduceId * 8)//跳到本次Block的数据区
      val offset = in.readLong()//数据文件中的开始位置
      val nextOffset = in.readLong()//数据文件中的结束位置
      new FileSegmentManagedBuffer(//
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,//数据文件中的开始位置
        nextOffset - offset)//数据文件中的结束位置
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store and DiskBlockObjectWriter.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  // TODO: Avoid this entirely by having the DiskBlockObjectWriter not require a BlockId.
  val NOOP_REDUCE_ID = 0
}
