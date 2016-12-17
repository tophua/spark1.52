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

import java.io.{Closeable, EOFException}
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging

/**
 * A reader for reading write ahead log files written using
 * 使用FileBasedWriteAheadLogWriter读取预写日志文件
 * [[org.apache.spark.streaming.util.FileBasedWriteAheadLogWriter]]. This reads
 * the records (bytebuffers) in the log file sequentially and return them as an
 * iterator of bytebuffers.
 * 这种读取记录(ByteBuffers)在日志文件中的顺序迭代器返回迭代遍历所有log
 * 
 */
private[streaming] class FileBasedWriteAheadLogReader(path: String, conf: Configuration)
  extends Iterator[ByteBuffer] with Closeable with Logging {

  private val instream = HdfsUtils.getInputStream(path, conf)
  private var closed = false
  private var nextItem: Option[ByteBuffer] = None

  override def hasNext: Boolean = synchronized {
    if (closed) {
       //如果已关闭,就肯定不hasNext了
      return false
    }
  
    if (nextItem.isDefined) { // handle the case where hasNext is called without calling next
      true
    } else {
      try {
         //读出来下一条,如果有,就说明还确实 hasNext
        val length = instream.readInt()
        val buffer = new Array[Byte](length)
        instream.readFully(buffer)
        nextItem = Some(ByteBuffer.wrap(buffer))
        logTrace("Read next item " + nextItem.get)
        true
      } catch {
        case e: EOFException =>
          logDebug("Error reading next item, EOF reached", e)
          close()
          false
        case e: Exception =>
          logWarning("Error while trying to read data from HDFS.", e)
          close()
          throw e
      }
    }
  }

  override def next(): ByteBuffer = synchronized {
    val data = nextItem.getOrElse {
      close()
      throw new IllegalStateException(
        "next called without calling hasNext or after hasNext returned false")
    }
    //确保下一个调用hasNext加载新的数据
    nextItem = None // Ensure the next hasNext call loads new data.
    data
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      instream.close()
    }
    closed = true
  }
}
