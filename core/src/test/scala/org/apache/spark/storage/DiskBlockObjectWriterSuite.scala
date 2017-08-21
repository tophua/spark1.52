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

import java.io.File

import org.apache.commons.io.{FileUtils, LineIterator}
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils
//用于将JVM对象直接写入磁盘上的文件的类
//支持直接写入一个文件到Disk,并且还支持文件的append,实际上它是org.apache.spark.storage.BlockObjectWriter的一个实现。
// 现在下面的类在需要Spill数据到Disk时,就是通过它来完成的
class DiskBlockObjectWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  var tempDir: File = _

  override def beforeEach(): Unit = {
    tempDir = Utils.createTempDir()
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(tempDir)
  }

  test("verify write metrics") {//验证写度量
    val file = new File(tempDir, "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    //注意os => os 匿名方法传递os方法名称
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)
    //Long.box将值类型转换为包装引用类型
    writer.write(Long.box(20), Long.box(30))
    // Record metrics update on every write
    //记录每一个写的度量更新
    assert(writeMetrics.shuffleRecordsWritten === 1)
    // Metrics don't update on every write
    //度量不更新在每一个写
    assert(writeMetrics.shuffleBytesWritten == 0)
    // After 32 writes, metrics should update
    //32写后,指标应该更新
    for (i <- 0 until 32) {
      writer.flush()
      //Long.box将值类型转换为包装引用类型
      writer.write(Long.box(i), Long.box(i))
    }
    assert(writeMetrics.shuffleBytesWritten > 0)
    assert(writeMetrics.shuffleRecordsWritten === 33)
    writer.commitAndClose()
    assert(file.length() == writeMetrics.shuffleBytesWritten)
  }

  test("verify write metrics on revert") {//在还原上验证写度量
    val file = new File(tempDir, "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)

    writer.write(Long.box(20), Long.box(30))
    // Record metrics update on every write
    //记录每一个写的度量更新
    assert(writeMetrics.shuffleRecordsWritten === 1)
    // Metrics don't update on every write
    //每次写入时，指标都不会更新
    assert(writeMetrics.shuffleBytesWritten == 0)
    // After 32 writes, metrics should update
    //32写后，指标应更新
    for (i <- 0 until 32) {
      writer.flush()
      writer.write(Long.box(i), Long.box(i))
    }
    assert(writeMetrics.shuffleBytesWritten > 0)
    assert(writeMetrics.shuffleRecordsWritten === 33)
    writer.revertPartialWritesAndClose()
    assert(writeMetrics.shuffleBytesWritten == 0)
    assert(writeMetrics.shuffleRecordsWritten == 0)
  }

  test("Reopening a closed block writer") {//重开一个关闭写的块
    val file = new File(tempDir, "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)

    writer.open()
    writer.close()
    intercept[IllegalStateException] {
      writer.open()
    }
  }
  //一个封闭的块写器应该没有效果
  test("calling revertPartialWritesAndClose() on a closed block writer should have no effect") {
    val file = new File(tempDir, "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)
    for (i <- 1 to 1000) {
      writer.write(i, i)
    }
    writer.commitAndClose()
    val bytesWritten = writeMetrics.shuffleBytesWritten
    assert(writeMetrics.shuffleRecordsWritten === 1000)
    writer.revertPartialWritesAndClose()
    assert(writeMetrics.shuffleRecordsWritten === 1000)
    assert(writeMetrics.shuffleBytesWritten === bytesWritten)
  }
  //应该是幂等的
  test("commitAndClose() should be idempotent") {
    val file = new File(tempDir, "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)
    for (i <- 1 to 1000) {
      writer.write(i, i)
    }
    writer.commitAndClose()
    val bytesWritten = writeMetrics.shuffleBytesWritten
    val writeTime = writeMetrics.shuffleWriteTime
    assert(writeMetrics.shuffleRecordsWritten === 1000)
    writer.commitAndClose()
    assert(writeMetrics.shuffleRecordsWritten === 1000)
    assert(writeMetrics.shuffleBytesWritten === bytesWritten)
    assert(writeMetrics.shuffleWriteTime === writeTime)
  }
  //应该是幂等的
  test("revertPartialWritesAndClose() should be idempotent") {
    val file = new File(tempDir, "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)
    for (i <- 1 to 1000) {
      writer.write(i, i)
    }
    writer.revertPartialWritesAndClose()
    val bytesWritten = writeMetrics.shuffleBytesWritten
    val writeTime = writeMetrics.shuffleWriteTime
    assert(writeMetrics.shuffleRecordsWritten === 0)
    writer.revertPartialWritesAndClose()
    assert(writeMetrics.shuffleRecordsWritten === 0)
    assert(writeMetrics.shuffleBytesWritten === bytesWritten)
    assert(writeMetrics.shuffleWriteTime === writeTime)
  }
  //只能在commitAndClose（）被调用后调用
  test("fileSegment() can only be called after commitAndClose() has been called") {
    val file = new File(tempDir, "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)
    for (i <- 1 to 1000) {
      writer.write(i, i)
    }
    intercept[IllegalStateException] {
      writer.fileSegment()
    }
    //注意获取段信息需要调用commitAndClose方法,close方法不能获取段信息
    //writer.close()
    writer.commitAndClose()
    val  fileSegment=writer.fileSegment().file.getCanonicalPath

    val exampleFile = FileUtils.getFile(fileSegment)
    val iter = FileUtils.lineIterator(exampleFile)

   // System.out.println("Contents of exampleTxt...")
    //迭代每行内容
    while(iter.hasNext)  {
      println("\t" + iter.next)
    }
    iter.close()
  }

  test("commitAndClose() without ever opening or writing") {//没有打开或写
    val file = new File(tempDir, "somefile")
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(new TestBlockId("0"), file,
      new JavaSerializer(new SparkConf()).newInstance(), 1024, os => os, true, writeMetrics)
    writer.commitAndClose()
    //段的长度为0
    assert(writer.fileSegment().length === 0)
  }
}
