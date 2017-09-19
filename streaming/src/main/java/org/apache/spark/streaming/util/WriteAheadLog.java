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

package org.apache.spark.streaming.util;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * :: DeveloperApi ::
 *
 * This abstract class represents a write ahead log (aka journal) that is used by Spark Streaming
 * to save the received data (by receivers) and associated metadata to a reliable storage, so that
 * they can be recovered after driver failures. See the Spark documentation for more information
 * on how to plug in your own custom implementation of a write ahead log.
 * 这个抽象类表示一个预写式日志(又名日志),用于Spark流将接收到的数据(由接收器)和相关元数据保存到可靠的存储器中,
 * 这样就可以在driver故障后恢复,有关如何插入自定义执行的预写式日的详细信息,请参见Spark文档
 * WriteAheadLog是多条log的集合,每条具体的 log的引用就是一个 LogRecordHandle
 */
@org.apache.spark.annotation.DeveloperApi
public abstract class WriteAheadLog {
  /**
   * Write the record to the log and return a record handle, which contains all the information
   * necessary to read back the written record. The time is used to the index the record,
   * 将记录写入日志并返回记录句柄,其中包含所有必要读写记录的信息,时间是用来索引记录
   * such that it can be cleaned later. Note that implementations of this abstract class must
   * ensure that the written data is durable and readable (using the record handle) by the
   * time this function returns.
   * 这样,它可以被清理后,注意,这个抽象类的实现必须确保函数返回的时候,写入数据是持久的和可读的(使用记录句柄)
   */
  abstract public WriteAheadLogRecordHandle write(ByteBuffer record, long time);

  /**
   * Read a written record based on the given record handle.
   * 根据给定的记录句柄读取记录
   */
  abstract public ByteBuffer read(WriteAheadLogRecordHandle handle);

  /**
   * Read and return an iterator of all the records that have been written but not yet cleaned up.
   * 读取并返回已写入所有记录的迭代器.但尚未清理
   */
  abstract public Iterator<ByteBuffer> readAll();

  /**
   * Clean all the records that are older than the threshold time. It can wait for
   * the completion of the deletion.
   * 清除所有大于阈值时间的记录,它可以等待完成删除。
   */
  abstract public void clean(long threshTime, boolean waitForCompletion);

  /**
   * Close this log and release any resources.
   * 关闭此日志并释放任何资源
   */
  abstract public void close();
}
