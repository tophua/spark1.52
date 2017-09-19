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

package org.apache.spark.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkFunSuite}

class CompressionCodecSuite extends SparkFunSuite {
  val conf = new SparkConf(false)

  def testCodec(codec: CompressionCodec) {
    // Write 1000 integers to the output stream, compressed.
    //将1000个整数写入输出流,压缩
    val outputStream = new ByteArrayOutputStream()
    val out = codec.compressedOutputStream(outputStream)
    for (i <- 1 until 1000) {
      out.write(i % 256)
    }
    out.close()

    // Read the 1000 integers back.
    //读1000个整数
    val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
    val in = codec.compressedInputStream(inputStream)
    for (i <- 1 until 1000) {
      assert(in.read() === i % 256)
    }
    in.close()
  }

  test("default compression codec") {//默认的压缩编解码器
    val codec = CompressionCodec.createCodec(conf)
    assert(codec.getClass === classOf[SnappyCompressionCodec])
    testCodec(codec)
  }

  test("lz4 compression codec") {//lz4压缩编解码器
    val codec = CompressionCodec.createCodec(conf, classOf[LZ4CompressionCodec].getName)
    assert(codec.getClass === classOf[LZ4CompressionCodec])
    testCodec(codec)
  }

  test("lz4 compression codec short form") {//lz4编解码压缩短形式
    val codec = CompressionCodec.createCodec(conf, "lz4")
    assert(codec.getClass === classOf[LZ4CompressionCodec])
    testCodec(codec)
  }
  //lz4不支持串行化流的连接
  test("lz4 does not support concatenation of serialized streams") {
    val codec = CompressionCodec.createCodec(conf, classOf[LZ4CompressionCodec].getName)
    assert(codec.getClass === classOf[LZ4CompressionCodec])
    intercept[Exception] {
      testConcatenationOfSerializedStreams(codec)
    }
  }

  test("lzf compression codec") {//lzf压缩编解码器
    val codec = CompressionCodec.createCodec(conf, classOf[LZFCompressionCodec].getName)
    assert(codec.getClass === classOf[LZFCompressionCodec])
    testCodec(codec)
  }

  test("lzf compression codec short form") {//lzf压缩编解码器短格式
    val codec = CompressionCodec.createCodec(conf, "lzf")
    assert(codec.getClass === classOf[LZFCompressionCodec])
    testCodec(codec)
  }
  //lzf支持串行化流的连接
  test("lzf supports concatenation of serialized streams") {
    val codec = CompressionCodec.createCodec(conf, classOf[LZFCompressionCodec].getName)
    assert(codec.getClass === classOf[LZFCompressionCodec])
    testConcatenationOfSerializedStreams(codec)
  }
  //snappy压缩编解码器
  test("snappy compression codec") {
    val codec = CompressionCodec.createCodec(conf, classOf[SnappyCompressionCodec].getName)
    assert(codec.getClass === classOf[SnappyCompressionCodec])
    testCodec(codec)
  }
  //snappy压缩编解码器短格式
  test("snappy compression codec short form") {
    val codec = CompressionCodec.createCodec(conf, "snappy")
    assert(codec.getClass === classOf[SnappyCompressionCodec])
    testCodec(codec)
  }
  //snappy不支持串行化流的连接
  test("snappy does not support concatenation of serialized streams") {
    val codec = CompressionCodec.createCodec(conf, classOf[SnappyCompressionCodec].getName)
    assert(codec.getClass === classOf[SnappyCompressionCodec])
    intercept[Exception] {
      testConcatenationOfSerializedStreams(codec)
    }
  }
  //坏的压缩编解码器
  test("bad compression codec") {
    intercept[IllegalArgumentException] {
      CompressionCodec.createCodec(conf, "foobar")
    }
  }
  //测试序列化流的连接
  private def testConcatenationOfSerializedStreams(codec: CompressionCodec): Unit = {
    val bytes1: Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val out = codec.compressedOutputStream(baos)
      (0 to 64).foreach(out.write)
      out.close()
      baos.toByteArray
    }
    val bytes2: Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val out = codec.compressedOutputStream(baos)
      (65 to 127).foreach(out.write)
      out.close()
      baos.toByteArray
    }
    val concatenatedBytes = codec.compressedInputStream(new ByteArrayInputStream(bytes1 ++ bytes2))
    val decompressed: Array[Byte] = new Array[Byte](128)
    ByteStreams.readFully(concatenatedBytes, decompressed)
    assert(decompressed.toSeq === (0 to 127))
  }
}
