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
package org.apache.spark.streaming.util

import java.io._
import java.nio.ByteBuffer

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream

/**
 * A writer for writing byte-buffers to a write ahead log file.
 * 一个预写式日志文件的字节缓冲
 * 就是给定一个文件、给定一个块数据,将数据写到文件里面去
 */
private[streaming] class FileBasedWriteAheadLogWriter(path: String, hadoopConf: Configuration)
  extends Closeable {

  private lazy val stream = HdfsUtils.getOutputStream(path, hadoopConf)

  private lazy val hadoopFlushMethod = {
    // Use reflection to get the right flush operation
    //使用反射得到正确的刷新操作
    val cls = classOf[FSDataOutputStream]
    //在具体的写 HDFS数据块的时候,需要判断一下具体用的方法,优先使用 hflush(),没有的话就使用 sync()
    Try(cls.getMethod("hflush")).orElse(Try(cls.getMethod("sync"))).toOption
  }

  private var nextOffset = stream.getPos()
  private var closed = false

  /** 
   *  Write the bytebuffer to the log file 
   *  写缓冲区的日志文件
   *  */
  def write(data: ByteBuffer): FileBasedWriteAheadLogSegment = synchronized {
    assertOpen()
    //确保缓冲区中的所有数据检索
    data.rewind() // Rewind to ensure all data in the buffer is retrieved
    val lengthToWrite = data.remaining()
    //数据写到文件完成后,记录一下文件 path、offset 和 length,封装为一个 FileBasedWriteAheadLogSegment返回
    val segment = new FileBasedWriteAheadLogSegment(path, nextOffset, lengthToWrite)
    stream.writeInt(lengthToWrite)
    if (data.hasArray) {
      stream.write(data.array())
    } else {
      // If the buffer is not backed by an array, we transfer using temp array
      //如果缓冲区没有一个数组支持,我们使用临时数组传输
      // Note that despite the extra array copy, this should be faster than byte-by-byte copy
      //请注意,尽管额外的数组副本,这应该是更快比字节字节的副本
      while (data.hasRemaining) {
        val array = new Array[Byte](data.remaining)
        data.get(array)
        stream.write(array)
      }
    }
    flush()
    nextOffset = stream.getPos()
    //数据写到文件完成后,记录一下文件 path、offset 和 length,封装为一个 FileBasedWriteAheadLogSegment返回
    segment
  }

  override def close(): Unit = synchronized {
    closed = true
    stream.close()
  }

  private def flush() {
    hadoopFlushMethod.foreach { _.invoke(stream) }
    // Useful for local file system where hflush/sync does not work (HADOOP-7844)
    stream.getWrappedStream.flush()
  }

  private def assertOpen() {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Writer to write to file.")
  }
}
