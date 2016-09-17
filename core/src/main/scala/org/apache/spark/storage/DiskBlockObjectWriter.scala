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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, FileOutputStream, File, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializerInstance, SerializationStream}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block and can guarantee atomicity in the case of faults as it allows the caller to
 * revert partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 * 用于Spark任务的中间计算结果输入文件,直接向一个文件写入数据,如果文件已经存在,那么会以追加的方式写入.
 */
private[spark] class DiskBlockObjectWriter(
    val blockId: BlockId,
    val file: File,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics)
  extends OutputStream
  with Logging {

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  //记录每条数据写入花费的时间
  private var ts: TimeTrackingOutputStream = null
  //压缩算法流
  private var objOut: SerializationStream = null
  //是否初始化
  private var initialized = false
  //是否关闭
  private var hasBeenClosed = false
  //
  private var commitAndCloseHasBeenCalled = false

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxx|--------|---       |
   *         ^        ^          ^
   *         |        |        finalPosition
   *         |      reportedPosition
   *       initialPosition
   *
   * initialPosition: Offset in the file where we start writing. Immutable.
   * reportedPosition: Position at the time of the last update to the write metrics.
   * finalPosition: Offset where we stopped writing. Set on closeAndCommit() then never changed.
   * -----: Current writes to the underlying file.
   * xxxxx: Existing contents of the file.
   */
  //Block在File中开始的位置，不变量，值为file.length()，即位File中已经被其他Block写入的数据量
  private val initialPosition = file.length() 
  //Block在File中结束的位置，初始值为-1，当调用commitAndClose方法时更新为当前File的大小，然后不可再改变。
  private var finalPosition: Long = -1
  //当前数据写入的位置，初始值为initialPosition，每写入32条数据时更新为channel.position()
  private var reportedPosition = initialPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private var numRecordsWritten = 0
/**
 * 初始化各个输出流, 打一个文件输出流,利用NIO,压缩,缓存,序列化方式打开一个文件输出流
 * 
 */
  def open(): DiskBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    fos = new FileOutputStream(file, true)
    //记录每条数据写入花费的时间
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    channel = fos.getChannel()
    //压缩算法流
    bs = compressStream(new BufferedOutputStream(ts, bufferSize))
    objOut = serializerInstance.serializeStream(bs)
    initialized = true
    this
  }
//关闭文件输出流,并更新测量信息
  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        if (syncWrites) {
          // Force outstanding writes to disk and track how long it takes
          objOut.flush()
          val start = System.nanoTime()
          fos.getFD.sync()//
          writeMetrics.incShuffleWriteTime(System.nanoTime() - start)
        }
      } {
        objOut.close()
      }

      channel = null
      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
      hasBeenClosed = true
    }
  }
  //判断文件是否已经打开
  def isOpen: Boolean = objOut != null

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * 将缓存数据写入磁盘并关闭缓存,并更新finalPosition,然后更新测量数据
   * 
   */
  def commitAndClose(): Unit = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      close()
      finalPosition = file.length()
      // In certain compression codecs, more bytes are written after close() is called
      writeMetrics.incShuffleBytesWritten(finalPosition - reportedPosition)
    } else {
      finalPosition = file.length()
    }
    commitAndCloseHasBeenCalled = true
  }


  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   * 撤销所有的写入操作，将文件中的内容恢复到写入数据之前。
   */
  def revertPartialWritesAndClose() {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    try {
      if (initialized) {
        writeMetrics.decShuffleBytesWritten(reportedPosition - initialPosition)
        writeMetrics.decShuffleRecordsWritten(numRecordsWritten)
        objOut.flush()
        bs.flush()
        close()
      }

      val truncateStream = new FileOutputStream(file, true)
      try {
        truncateStream.getChannel.truncate(initialPosition)
      } finally {
        truncateStream.close()
      }
    } catch {
      case e: Exception =>
        logError("Uncaught exception while reverting partial writes to file " + file, e)
    }
  }

  /**
   * Writes a key-value pair.
   * 用于将数据写入文件,并更新测量信息
   */
  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }
   //写入一条数据
  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!initialized) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   * 
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incShuffleRecordsWritten(1)

    if (numRecordsWritten % 32 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * Returns the file segment of committed data that this Writer has written.
   * This is only valid after commitAndClose() has been called.
   * 返回FileSegment，该方法只有在commitAndClose方法调用之后才有效。
   * FileSegment表示文件的一个连续的片段
   */
  def fileSegment(): FileSegment = {
    if (!commitAndCloseHasBeenCalled) {
      throw new IllegalStateException(
        "fileSegment() is only valid after commitAndClose() has been called")
    }
    new FileSegment(file, initialPosition, finalPosition - initialPosition)
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   * 更新测量信息 
   */
  private def updateBytesWritten() {
    val pos = channel.position()
    writeMetrics.incShuffleBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
