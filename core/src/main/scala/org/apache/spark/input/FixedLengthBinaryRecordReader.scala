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

package org.apache.spark.input

import java.io.IOException

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * FixedLengthBinaryRecordReader is returned by FixedLengthBinaryInputFormat.
 * It uses the record length set in FixedLengthBinaryInputFormat to
 * read one record at a time from the given InputSplit.
 *
 * Each call to nextKeyValue() updates the LongWritable key and BytesWritable value.
 *
 * key = record index (Long)
 * value = the record itself (BytesWritable)
 */
private[spark] class FixedLengthBinaryRecordReader
  extends RecordReader[LongWritable, BytesWritable] {

  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var currentPosition: Long = 0L
  private var recordLength: Int = 0
  private var fileInputStream: FSDataInputStream = null
  private var recordKey: LongWritable = null
  private var recordValue: BytesWritable = null

  override def close() {
    if (fileInputStream != null) {
      fileInputStream.close()
    }
  }

  override def getCurrentKey: LongWritable = {
    recordKey
  }

  override def getCurrentValue: BytesWritable = {
    recordValue
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        ((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    // the file input 文件输入
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // the byte position this fileSplit starts at
    //这个文件分割开始的字节位置
    splitStart = fileSplit.getStart

    // splitEnd byte marker that the fileSplit ends at
    //分裂结束字节标记,文件拆分的结束位置
    splitEnd = splitStart + fileSplit.getLength

    // the actual file we will be reading from
    //我们将从中读取的实际文件
    val file = fileSplit.getPath
    // job configuration 作业配置
    val job = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    // check compression 检查配置
    val codec = new CompressionCodecFactory(job).getCodec(file)
    if (codec != null) {
      throw new IOException("FixedLengthRecordReader does not support reading compressed files")
    }
    // get the record length 获取记录长度
    recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)
    // get the filesystem 获得系统文件
    val fs = file.getFileSystem(job)
    // open the File 打开文件
    fileInputStream = fs.open(file)
    // seek to the splitStart position
    //寻找分裂的起始位置
    fileInputStream.seek(splitStart)
    // set our current position
    //设置我们当前的位置
    currentPosition = splitStart
  }

  override def nextKeyValue(): Boolean = {
    if (recordKey == null) {
      recordKey = new LongWritable()
    }
    // the key is a linear index of the record, given by the
    //键是记录的一个线性索引,由位置给定的记录开始除以记录长度
    // position the record starts divided by the record length
    recordKey.set(currentPosition / recordLength)
    // the recordValue to place the bytes into
    //将字节放置到的记录值
    if (recordValue == null) {
      recordValue = new BytesWritable(new Array[Byte](recordLength))
    }
    // read a record if the currentPosition is less than the split end
    //读取记录,如果当前位置小于分分裂结束
    if (currentPosition < splitEnd) {
      // setup a buffer to store the record
      //设置一个缓冲区来存储记录
      val buffer = recordValue.getBytes
      fileInputStream.readFully(buffer)
      // update our current position
      //更新我们目前的位置
      currentPosition = currentPosition + recordLength
      // return true
      return true
    }
    false
  }
}
