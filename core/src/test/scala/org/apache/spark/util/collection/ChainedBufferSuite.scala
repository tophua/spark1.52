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

package org.apache.spark.util.collection

import java.nio.ByteBuffer

import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite

class ChainedBufferSuite extends SparkFunSuite {
  test("write and read at start") {//开始写和读
    // write from start of source array
    //从数组开始写
    val buffer = new ChainedBuffer(8)
    buffer.capacity should be (0)
    verifyWriteAndRead(buffer, 0, 0, 0, 4)
    buffer.capacity should be (8)

    // write from middle of source array
    //从源数组中间写
    verifyWriteAndRead(buffer, 0, 5, 0, 4)
    buffer.capacity should be (8)

    // read to middle of target array
    //读取目标数组中间值
    verifyWriteAndRead(buffer, 0, 0, 5, 4)
    buffer.capacity should be (8)

    // write up to border 写边界
    verifyWriteAndRead(buffer, 0, 0, 0, 8)
    buffer.capacity should be (8)

    // expand into second buffer
    //扩展到第二个缓冲区
    verifyWriteAndRead(buffer, 0, 0, 0, 12)
    buffer.capacity should be (16)

    // expand into multiple buffers
    //扩展到多个缓冲区
    verifyWriteAndRead(buffer, 0, 0, 0, 28)
    buffer.capacity should be (32)
  }

  test("write and read at middle") {//在中间写和读
    val buffer = new ChainedBuffer(8)

    // fill to a middle point 填充中间点
    verifyWriteAndRead(buffer, 0, 0, 0, 3)

    // write from start of source array
    //从数组开始写
    verifyWriteAndRead(buffer, 3, 0, 0, 4)
    buffer.capacity should be (8)

    // write from middle of source array
    //从源数组中间写
    verifyWriteAndRead(buffer, 3, 5, 0, 4)
    buffer.capacity should be (8)

    // read to middle of target array
    //读到目标数组的中间
    verifyWriteAndRead(buffer, 3, 0, 5, 4)
    buffer.capacity should be (8)

    // write up to border 写边界
    verifyWriteAndRead(buffer, 3, 0, 0, 5)
    buffer.capacity should be (8)

    // expand into second buffer 扩展到第二个缓冲区
    verifyWriteAndRead(buffer, 3, 0, 0, 12)
    buffer.capacity should be (16)

    // expand into multiple buffers 扩展到多个缓冲区
    verifyWriteAndRead(buffer, 3, 0, 0, 28)
    buffer.capacity should be (32)
  }

  test("write and read at later buffer") {//在缓冲区中写入和读取
    val buffer = new ChainedBuffer(8)

    // fill to a middle point 填充中间点
    verifyWriteAndRead(buffer, 0, 0, 0, 11)

    // write from start of source array
    //从数组开始写
    verifyWriteAndRead(buffer, 11, 0, 0, 4)
    buffer.capacity should be (16)

    // write from middle of source array
    //从源数组中间写
    verifyWriteAndRead(buffer, 11, 5, 0, 4)
    buffer.capacity should be (16)

    // read to middle of target array
    //读到目标数组的中间
    verifyWriteAndRead(buffer, 11, 0, 5, 4)
    buffer.capacity should be (16)

    // write up to border 写边界
    verifyWriteAndRead(buffer, 11, 0, 0, 5)
    buffer.capacity should be (16)

    // expand into second buffer 扩展到第二个缓冲区
    verifyWriteAndRead(buffer, 11, 0, 0, 12)
    buffer.capacity should be (24)

    // expand into multiple buffers 扩展到多个缓冲区
    verifyWriteAndRead(buffer, 11, 0, 0, 28)
    buffer.capacity should be (40)
  }


  // Used to make sure we're writing different bytes each time
  //用来每一次确保我们写不同的字节
  var rangeStart = 0

  /**
   * @param buffer The buffer to write to and read from.
   * @param offsetInBuffer The offset to write to in the buffer.
   * @param offsetInSource The offset in the array that the bytes are written from.
   * @param offsetInTarget The offset in the array to read the bytes into.
   * @param length The number of bytes to read and write
   */
  def verifyWriteAndRead(
      buffer: ChainedBuffer,
      offsetInBuffer: Int,
      offsetInSource: Int,
      offsetInTarget: Int,
      length: Int): Unit = {
    val source = new Array[Byte](offsetInSource + length)
    (rangeStart until rangeStart + length).map(_.toByte).copyToArray(source, offsetInSource)
    buffer.write(offsetInBuffer, source, offsetInSource, length)
    val target = new Array[Byte](offsetInTarget + length)
    buffer.read(offsetInBuffer, target, offsetInTarget, length)
    ByteBuffer.wrap(source, offsetInSource, length) should be
      (ByteBuffer.wrap(target, offsetInTarget, length))

    rangeStart += 100
  }
}
