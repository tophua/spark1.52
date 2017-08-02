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

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.FileShuffleBlockResolver.ShuffleFileGroup
import org.apache.spark.storage._
import org.apache.spark.util.collection.{PrimitiveKeyOpenHashMap, PrimitiveVector}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap, Utils}
import org.apache.spark.{Logging, SparkConf, SparkEnv}

/** A group of writers for a ShuffleMapTask, one writer per reducer.
  * 一组写的ShuffleMapTask，每个写的reducer*/
private[spark] trait ShuffleWriterGroup {
  val writers: Array[DiskBlockObjectWriter]

  /** @param success Indicates all writes were successful. If false, no blocks will be recorded. */
  def releaseWriters(success: Boolean)
}

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets one file
 * per reducer (this set of files is called a ShuffleFileGroup).
 *
 * As an optimization to reduce the number of physical shuffle files produced, multiple shuffle
 * blocks are aggregated into the same file. There is one "combined shuffle file" per reducer
 * per concurrently executing shuffle task. As soon as a task finishes writing to its shuffle
 * files, it releases them for another task.
 * Regarding the implementation of this feature, shuffle files are identified by a 3-tuple:
 *   - shuffleId: The unique id given to the entire shuffle stage.
 *   - bucketId: The id of the output partition (i.e., reducer id)
 *   - fileId: The unique id identifying a group of "combined shuffle files." Only one task at a
 *       time owns a particular fileId, and this id is returned to a pool when the task finishes.
 * Each shuffle file is then mapped to a FileSegment, which is a 3-tuple (file, offset, length)
 * that specifies where in a given file the actual block data is located.
 *
 * Shuffle file metadata is stored in a space-efficient manner. Rather than simply mapping
 * ShuffleBlockIds directly to FileSegments, each ShuffleFileGroup maintains a list of offsets for
 * each block stored in each file. In order to find the location of a shuffle block, we search the
 * files within a ShuffleFileGroups associated with the block's reducer.
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getHashBasedShuffleBlockData().
private[spark] class FileShuffleBlockResolver(conf: SparkConf)
  extends ShuffleBlockResolver with Logging {

  private val transportConf = SparkTransportConf.fromSparkConf(conf)

  private lazy val blockManager = SparkEnv.get.blockManager

  // Turning off shuffle file consolidation causes all shuffle Blocks to get their own file.
  // TODO: Remove this once the shuffle file consolidation feature is stable.
  //如果为true,在shuffle时就合并中间文件,对于有大量Reduce任务的shuffle来说,合并文件可 以提高文件系统性能,
  //如果使用的是ext4 或 xfs 文件系统,建议设置为true；对于ext3,由于文件系统的限制,设置为true反而会使内核>8的机器降低性能
  val consolidateShuffleFiles =
    conf.getBoolean("spark.shuffle.consolidateFiles", false)

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  // 在ShuffleMapTask端通常也会增大Map任务的写磁盘的缓存,默认情况下是32K
  private val bufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  /**
   * Contains all the state related to a particular shuffle. This includes a pool of unused
   * ShuffleFileGroups, as well as all ShuffleFileGroups that have been created for the shuffle.
    * 包含与特定随机播放相关的所有状态,这包括一个未使用的ShuffleFileGroups池,以及为洗牌创建的所有ShuffleFileGroups。
   */
  private class ShuffleState(val numBuckets: Int) {
    val nextFileId = new AtomicInteger(0)
    val unusedFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()
    val allFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()

    /**
     * The mapIds of all map tasks completed on this Executor for this shuffle.
     * NB: This is only populated if consolidateShuffleFiles is FALSE. We don't need it otherwise.
      * 所有map任务的mapIds在此执行器上完成,用于此shuffle。
        注意：只有在整合ShuffleFiles为FALSE时,此填充才会被填充,否则我们不需要它
     */
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }

  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  /**
   * 
   * Get a ShuffleWriterGroup for the given map task, which will register it as complete
   * when the writers are closed successfully
    * 为给定的地图任务获取一个ShuffleWriterGroup,当作者关闭成功时,它将注册为完整的
   * 
   */
  def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer,
      writeMetrics: ShuffleWriteMetrics): ShuffleWriterGroup = {
    new ShuffleWriterGroup {
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets))
      private val shuffleState = shuffleStates(shuffleId)
      private var fileGroup: ShuffleFileGroup = null

      val openStartTime = System.nanoTime
      val serializerInstance = serializer.newInstance()
      //如果consolidateShuffleFiles为true,那么在一个Task中,有多少个输出的Partition就会有多少个中间文件,默认为false
      val writers: Array[DiskBlockObjectWriter] = if (consolidateShuffleFiles) {
        fileGroup = getUnusedFileGroup()//获取没有使用的FileGroup
        Array.tabulate[DiskBlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          blockManager.getDiskWriter(blockId, fileGroup(bucketId), serializerInstance, bufferSize,
            writeMetrics)
        }
      } else {
        Array.tabulate[DiskBlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          //如果blockFile已经存在,那么删除它并打印日志
          val blockFile = blockManager.diskBlockManager.getFile(blockId)           
          val tmp = Utils.tempFileWith(blockFile)    
          //tmp也就是blockFile如果已经存在则,在后面追加数据
          blockManager.getDiskWriter(blockId, tmp, serializerInstance, bufferSize, writeMetrics)
        }
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, so should be included in the shuffle write time.
      //创建要写入和创建磁盘刻录机的文件都涉及与磁盘交互,因此应该包含在shuffle写入的时间。
      writeMetrics.incShuffleWriteTime(System.nanoTime - openStartTime)

      override def releaseWriters(success: Boolean) {
        if (consolidateShuffleFiles) {
          if (success) {
            val offsets = writers.map(_.fileSegment().offset)
            val lengths = writers.map(_.fileSegment().length)
            fileGroup.recordMapOutput(mapId, offsets, lengths)
          }
          recycleFileGroup(fileGroup)
        } else {
          shuffleState.completedMapTasks.add(mapId)
        }
      }

      private def getUnusedFileGroup(): ShuffleFileGroup = {
        val fileGroup = shuffleState.unusedFileGroups.poll()
        if (fileGroup != null) fileGroup else newFileGroup()
      }

      private def newFileGroup(): ShuffleFileGroup = {
        val fileId = shuffleState.nextFileId.getAndIncrement()
        val files = Array.tabulate[File](numBuckets) { bucketId =>
          val filename = physicalFileName(shuffleId, bucketId, fileId)
          blockManager.diskBlockManager.getFile(filename)
        }
        val fileGroup = new ShuffleFileGroup(shuffleId, fileId, files)
        shuffleState.allFileGroups.add(fileGroup)
        fileGroup
      }

      private def recycleFileGroup(group: ShuffleFileGroup) {
        shuffleState.unusedFileGroups.add(group)
      }
    }
  }
/**
 * 核心的读取逻辑
 */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    if (consolidateShuffleFiles) {
      // Search all file groups associated with this shuffle.
      val shuffleState = shuffleStates(blockId.shuffleId)
      val iter = shuffleState.allFileGroups.iterator
      while (iter.hasNext) {
      //根据Map ID和Reduce ID获取File Segment的信息
        val segmentOpt = iter.next.getFileSegmentFor(blockId.mapId, blockId.reduceId)
        if (segmentOpt.isDefined) {
          val segment = segmentOpt.get
	  //根据File Segment的信息,从FileGroup中到相应的File和Block在文件中的offset和size
	  return new FileSegmentManagedBuffer(
            transportConf, segment.file, segment.offset, segment.length)
        }
      }
      throw new IllegalStateException("Failed to find shuffle block: " + blockId)
    } else {
      val file = blockManager.diskBlockManager.getFile(blockId)//直接获取文件句柄
      new FileSegmentManagedBuffer(transportConf, file, 0, file.length)
    }
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle.
    * 删除与特定shuffle相关的所有块/文件和元数据。*/
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this, if shuffleStates should be removed only
    // after the corresponding shuffle blocks have been removed
    //不要改变这个的顺序,如果shuffleStates只有在相应的shuffle块被删除之后才被删除
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  /** Remove all the blocks / files related to a particular shuffle.
    * 删除与特定shuffle相关的所有块/文件*/
  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        if (consolidateShuffleFiles) {
          for (fileGroup <- state.allFileGroups; file <- fileGroup.files) {
            file.delete()
          }
        } else {
          for (mapId <- state.completedMapTasks; reduceId <- 0 until state.numBuckets) {
            val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
            blockManager.diskBlockManager.getFile(blockId).delete()
          }
        }
        logInfo("Deleted all files for shuffle " + shuffleId)
        true
      case None =>
        logInfo("Could not find files for shuffle " + shuffleId + " for deleting")
        false
    }
  }

  private def physicalFileName(shuffleId: Int, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%d_%d".format(shuffleId, bucketId, fileId)
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  override def stop() {
    metadataCleaner.cancel()
  }
}

private[spark] object FileShuffleBlockResolver {
  /**
   * 一组shuffle文件,这个文件组的每个文件都对应一个partition或者下游的task
   * 因此一个Shuffle Map Task来说对应一个文件
   * A group of shuffle files, one per reducer.
   * A particular mapper will be assigned a single ShuffleFileGroup to write its output to.
   */
  private class ShuffleFileGroup(val shuffleId: Int, val fileId: Int, val files: Array[File]) {
    private var numBlocks: Int = 0

    /**
     * Stores the absolute index of each mapId in the files of this group. For instance,
     * if mapId 5 is the first block in each file, mapIdToIndex(5) = 0.
      * 将每个mapId的绝对索引存储在此组的文件中,例如,如果mapId 5是每个文件中的第一个块,则mapIdToIndex（5）= 0
     */
    private val mapIdToIndex = new PrimitiveKeyOpenHashMap[Int, Int]()

    /**
     * Stores consecutive offsets and lengths of blocks into each reducer file, ordered by
     * position in the file.
      * 将连续的偏移量和块长度存储到每个reducer文件中,按文件中的位置排序
     * Note: mapIdToIndex(mapId) returns the index of the mapper into the vector for every
     * reducer.
      * 注意：mapIdToIndex(mapId)将映射器的索引返回给每个reducer的向量。
     */
    private val blockOffsetsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }
    private val blockLengthsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }

    def apply(bucketId: Int): File = files(bucketId)

    def recordMapOutput(mapId: Int, offsets: Array[Long], lengths: Array[Long]) {
      assert(offsets.length == lengths.length)
      mapIdToIndex(mapId) = numBlocks
      numBlocks += 1
      for (i <- 0 until offsets.length) {
        blockOffsetsByReducer(i) += offsets(i)
        blockLengthsByReducer(i) += lengths(i)
      }
    }

    /** Returns the FileSegment associated with the given map task, or None if no entry exists.
      * 返回与给定地图任务关联的FileSegment,如果没有条目,则返回None。 */
    def getFileSegmentFor(mapId: Int, reducerId: Int): Option[FileSegment] = {
      val file = files(reducerId)
      val blockOffsets = blockOffsetsByReducer(reducerId)
      val blockLengths = blockLengthsByReducer(reducerId)
      val index = mapIdToIndex.getOrElse(mapId, -1)
      if (index >= 0) {
        val offset = blockOffsets(index)
        val length = blockLengths(index)
        Some(new FileSegment(file, offset, length))
      } else {
        None
      }
    }
  }
}
