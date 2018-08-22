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

package org.apache.spark.sql.execution

import java.io.{File, ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.Utils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark._


/**
 * used to test close InputStream in UnsafeRowSerializer 
 * 用于测试在关闭输入流不安全的行序列化
 * 关闭输入流的字节数组
 */
class ClosableByteArrayInputStream(buf: Array[Byte]) extends ByteArrayInputStream(buf) {
  var closed: Boolean = false
  override def close(): Unit = {
    closed = true
    super.close()
  }
}
//不安全的行序列化测试套件
class UnsafeRowSerializerSuite extends SparkFunSuite with LocalSparkContext {

  private def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }
  //不安全的行转换化器
  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }
  //tounsaferow()测试辅助方法
  test("toUnsafeRow() test helper method") {
    // This currently doesnt work because the generic getter throws an exception.
    //目前不工作因为一般人抛出一个异常
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    assert(row.getString(0) === unsafeRow.getUTF8String(0).toString)
    assert(row.getInt(1) === unsafeRow.getInt(1))
  }

  test("basic row serialization") {//基本行序列化
    val rows = Seq(Row("Hello", 1), Row("World", 2))
    val unsafeRows = rows.map(row => toUnsafeRow(row, Array(StringType, IntegerType)))
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val baos = new ByteArrayOutputStream()
    val serializerStream = serializer.serializeStream(baos)
    for (unsafeRow <- unsafeRows) {
      serializerStream.writeKey(0)
      serializerStream.writeValue(unsafeRow)
    }
    serializerStream.close()
    val input = new ClosableByteArrayInputStream(baos.toByteArray)
    val deserializerIter = serializer.deserializeStream(input).asKeyValueIterator
    for (expectedRow <- unsafeRows) {
      val actualRow = deserializerIter.next().asInstanceOf[(Integer, UnsafeRow)]._2
      assert(expectedRow.getSizeInBytes === actualRow.getSizeInBytes)
      assert(expectedRow.getString(0) === actualRow.getString(0))
      assert(expectedRow.getInt(1) === actualRow.getInt(1))
    }
    assert(!deserializerIter.hasNext)
    assert(input.closed)
  }

  test("close empty input stream") {//关闭空输入流
    val input = new ClosableByteArrayInputStream(Array.empty)
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val deserializerIter = serializer.deserializeStream(input).asKeyValueIterator
    assert(!deserializerIter.hasNext)
    assert(input.closed)
  }
   //外部排序溢出行序列化程序不安全
  test("SPARK-10466: external sorter spilling with unsafe row serializer") {
    var sc: SparkContext = null
    var outputFile: File = null
    val oldEnv = SparkEnv.get // save the old SparkEnv, as it will be overwritten
    Utils.tryWithSafeFinally {
      val conf = new SparkConf()
        .set("spark.shuffle.spill.initialMemoryThreshold", "1024")
        //用于设置在Reducer的partition数目少于多少的时候,Sort Based Shuffle内部不使用Merge Sort的方式处理数据,而是直接将每个partition写入单独的文件
        .set("spark.shuffle.sort.bypassMergeThreshold", "0")
        //Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill(临时写入外部存储或一直使用内存)
        .set("spark.shuffle.memoryFraction", "0.0001")

      sc = new SparkContext("local", "test", conf)
      outputFile = File.createTempFile("test-unsafe-row-serializer-spill", "")
      // prepare data 准备数据
      val converter = unsafeRowConverter(Array(IntegerType))
      val data = (1 to 1000).iterator.map { i =>
        (i, converter(Row(i)))
      }
      val sorter = new ExternalSorter[Int, UnsafeRow, UnsafeRow](
        partitioner = Some(new HashPartitioner(10)),
        serializer = Some(new UnsafeRowSerializer(numFields = 1)))

      // Ensure we spilled something and have to merge them later
      //确保我们溢出的东西,并将其合并后
      assert(sorter.numSpills === 0)
      sorter.insertAll(data)
      assert(sorter.numSpills > 0)

      // Merging spilled files should not throw assertion error
      //合并溢出的文件不应该抛出断言错误
      val taskContext =
        new TaskContextImpl(0, 0, 0, 0, null, null, InternalAccumulator.create(sc))
      taskContext.taskMetrics.shuffleWriteMetrics = Some(new ShuffleWriteMetrics)
      sorter.writePartitionedFile(ShuffleBlockId(0, 0, 0), taskContext, outputFile)
    } {
      // Clean up 清理
      if (sc != null) {
        sc.stop()
      }

      // restore the spark env
      //恢复Spark环境
      SparkEnv.set(oldEnv)

      if (outputFile != null) {
        outputFile.delete()
      }
    }
  }
  //安全与不安全的Shuffle行序列化
  ignore("SPARK-10403: unsafe row serializer with UnsafeShuffleManager") {
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "tungsten-sort")
    sc = new SparkContext("local", "test", conf)
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    val rowsRDD = sc.parallelize(Seq((0, unsafeRow), (1, unsafeRow), (0, unsafeRow)))
      .asInstanceOf[RDD[Product2[Int, InternalRow]]]
    val shuffled = new ShuffledRowRDD(rowsRDD, new UnsafeRowSerializer(2), 2)
    shuffled.count()
  }
}
