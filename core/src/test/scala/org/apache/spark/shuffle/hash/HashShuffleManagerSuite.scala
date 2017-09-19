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

import java.io.{File, FileWriter}

import scala.language.reflectiveCalls

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.FileShuffleBlockResolver
import org.apache.spark.storage.{ShuffleBlockId, FileSegment}

class HashShuffleManagerSuite extends SparkFunSuite with LocalSparkContext {
  private val testConf = new SparkConf(false)

  private def checkSegments(expected: FileSegment, buffer: ManagedBuffer) {
    assert(buffer.isInstanceOf[FileSegmentManagedBuffer])
    //asInstanceOf强制类型转换
    val segment = buffer.asInstanceOf[FileSegmentManagedBuffer]
    assert(expected.file.getCanonicalPath === segment.getFile.getCanonicalPath)
    assert(expected.offset === segment.getOffset)
    assert(expected.length === segment.getLength)
  }
  //合并Shuffle,写Shuffle分组现有的偏移/长度
  test("consolidated shuffle can write to shuffle group without messing existing offsets/lengths") {

    val conf = new SparkConf(false)
    // reset after EACH object write. This is to ensure that there are bytes appended after
    // an object is written. So if the codepaths assume writeObject is end of data, this should
    // flush those bugs out. This was common bug in ExternalAppendOnlyMap, etc.
    //EACH对象写入后复位,这是为了确保在写入对象后附加字节,所以如果codepaths认为writeObject是数据的结尾,那么应该将这些bug清除掉,
    // 这在ExternalAppendOnlyMap等中是常见的错误。
    conf.set("spark.serializer.objectStreamReset", "1")
    //如果为true,在shuffle时就合并中间文件,对于有大量Reduce任务的shuffle来说,合并文件可 以提高文件系统性能,
    //如果使用的是ext4 或 xfs 文件系统,建议设置为true；对于ext3,由于文件系统的限制,设置为true反而会使内核>8的机器降低性能
    conf.set("spark.shuffle.consolidateFiles", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.hash.HashShuffleManager")

    sc = new SparkContext("local", "test", conf)

    val shuffleBlockResolver =
      SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[FileShuffleBlockResolver]

    val shuffle1 = shuffleBlockResolver.forMapTask(1, 1, 1, new JavaSerializer(conf),
      new ShuffleWriteMetrics)
    for (writer <- shuffle1.writers) {
      writer.write("test1", "value")
      writer.write("test2", "value")
    }
    for (writer <- shuffle1.writers) {
      writer.commitAndClose()
    }

    val shuffle1Segment = shuffle1.writers(0).fileSegment()
    shuffle1.releaseWriters(success = true)

    val shuffle2 = shuffleBlockResolver.forMapTask(1, 2, 1, new JavaSerializer(conf),
      new ShuffleWriteMetrics)

    for (writer <- shuffle2.writers) {
      writer.write("test3", "value")
      writer.write("test4", "vlue")
    }
    for (writer <- shuffle2.writers) {
      writer.commitAndClose()
    }
    val shuffle2Segment = shuffle2.writers(0).fileSegment()
    shuffle2.releaseWriters(success = true)

    // Now comes the test :
    //现在考试来了：
    // Write to shuffle 3; and close it, but before registering it, check if the file lengths for
    // previous task (forof shuffle1) is the same as 'segments'. Earlier, we were inferring length
    // of block based on remaining data in file : which could mess things up when there is
    // concurrent read and writes happening to the same shuffle group.
    //写到洗牌3；并关闭它，但在注册它之前，检查文件长度是否为以前的任务（给shuffle1）是为“段相同。早些时候，我们推断长度。
    //块基于文件中剩余的数据：当有数据时可能会把事情弄得一团糟并发读取和写入发生在同一个洗牌组中。
    val shuffle3 = shuffleBlockResolver.forMapTask(1, 3, 1, new JavaSerializer(testConf),
      new ShuffleWriteMetrics)
    for (writer <- shuffle3.writers) {
      writer.write("test3", "value")
      writer.write("test4", "value")
    }
    for (writer <- shuffle3.writers) {
      writer.commitAndClose()
    }
    // check before we register.
    //注册之前检查
    checkSegments(shuffle2Segment, shuffleBlockResolver.getBlockData(ShuffleBlockId(1, 2, 0)))
    shuffle3.releaseWriters(success = true)
    checkSegments(shuffle2Segment, shuffleBlockResolver.getBlockData(ShuffleBlockId(1, 2, 0)))
    shuffleBlockResolver.removeShuffle(1)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }
}
