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

package org.apache.spark.shuffle.hash

import java.io.IOException

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.storage.DiskBlockObjectWriter

private[spark] class HashShuffleWriter[K, V](
    shuffleBlockResolver: FileShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions
  private val metrics = context.taskMetrics

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private val writeMetrics = new ShuffleWriteMetrics()
  metrics.shuffleWriteMetrics = Some(writeMetrics)

  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  //
  private val shuffle = shuffleBlockResolver.forMapTask(dep.shuffleId, mapId, numOutputSplits, ser,
    writeMetrics)

  /** Write a bunch(形成一串) of records to this task's output */
    /**
     * 主要处理两件事:
     * 1)判断是否需要进行聚合,比如<hello,1>和<hello,1>都要写入的话,那么先生成<hello,2>
     *   然后再进行后续的写入工作
     * 2)利用Partition函数来决定<k,val>写入哪一个文件中.
     */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    //判断aggregator是否被定义
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {//判断是否需要聚合,如果需要，聚合records执行map端的聚合
        //汇聚工作,reducebyKey是一分为二的,一部在ShuffleMapTask中进行聚合
        //另一部分在resultTask中聚合
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }
     //利用getPartition函数来决定<k,val>写入哪一个文件中.
    for (elem <- iter) {
     //elem是类似于<k,val>的键值对,以K为参数用partitioner计算其对应的值,
     //从而选取相应的writer来写入整个elem
     //那么这里的partitioner是什么呢?默认HashPartitioner
      val bucketId = dep.partitioner.getPartition(elem._1)
      //
      shuffle.writers(bucketId).write(elem._1, elem._2)
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(initiallySuccess: Boolean): Option[MapStatus] = {
    var success = initiallySuccess
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        try {
          Some(commitWritesAndBuildStatus())
        } catch {
          case e: Exception =>
            success = false
            revertWrites()
            throw e
        }
      } else {
        revertWrites()
        None
      }
    } finally {
      // Release the writers back to the shuffle block manager.
      if (shuffle != null && shuffle.writers != null) {
        try {
          shuffle.releaseWriters(success)
        } catch {
          case e: Exception => logError("Failed to release shuffle writers", e)
        }
      }
    }
  }

  private def commitWritesAndBuildStatus(): MapStatus = {
    // Commit the writes. Get the size of each bucket block (total block size).
    val sizes: Array[Long] = shuffle.writers.map { writer: DiskBlockObjectWriter =>
      writer.commitAndClose()
      writer.fileSegment().length
    }
    if (!shuffleBlockResolver.consolidateShuffleFiles) {
      // rename all shuffle files to final paths
      // Note: there is only one ShuffleBlockResolver in executor
      shuffleBlockResolver.synchronized {
        shuffle.writers.zipWithIndex.foreach { case (writer, i) =>
          val output = blockManager.diskBlockManager.getFile(writer.blockId)
          if (sizes(i) > 0) {
            if (output.exists()) {
              // Use length of existing file and delete our own temporary one
              sizes(i) = output.length()
              writer.file.delete()
            } else {
              // Commit by renaming our temporary file to something the fetcher expects
              if (!writer.file.renameTo(output)) {
                throw new IOException(s"fail to rename ${writer.file} to $output")
              }
            }
          } else {
            if (output.exists()) {
              output.delete()
            }
          }
        }
      }
    }
    MapStatus(blockManager.shuffleServerId, sizes)
  }

  private def revertWrites(): Unit = {
    if (shuffle != null && shuffle.writers != null) {
      for (writer <- shuffle.writers) {
        writer.revertPartialWritesAndClose()
      }
    }
  }
}
