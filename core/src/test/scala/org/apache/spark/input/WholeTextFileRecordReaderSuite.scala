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

import java.io.DataOutputStream
import java.io.File
import java.io.FileOutputStream

import scala.collection.immutable.IndexedSeq

import org.scalatest.BeforeAndAfterAll

import org.apache.hadoop.io.Text

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.util.Utils
import org.apache.hadoop.io.compress.{DefaultCodec, CompressionCodecFactory, GzipCodec}

/**
 * Tests the correctness of
 * [[org.apache.spark.input.WholeTextFileRecordReader WholeTextFileRecordReader]]. A temporary
 * directory is created as fake input. Temporal storage would be deleted in the end.
 */
class WholeTextFileRecordReaderSuite extends SparkFunSuite with BeforeAndAfterAll with Logging {
  private var sc: SparkContext = _
  private var factory: CompressionCodecFactory = _

  override def beforeAll() {
    // Hadoop's FileSystem caching does not use the Configuration as part of its cache key, which
    // can cause Filesystem.get(Configuration) to return a cached instance created with a different
    // configuration than the one passed to get() (see HADOOP-8490 for more details). This caused
    // hard-to-reproduce test failures, since any suites that were run after this one would inherit
    // the new value of "fs.local.block.size" (see SPARK-5227 and SPARK-5679). To work around this,
    // we disable FileSystem caching in this suite.
    val conf = new SparkConf().set("spark.hadoop.fs.file.impl.disable.cache", "true")

    sc = new SparkContext("local", "test", conf)

    // Set the block size of local file system to test whether files are split right or not.
    sc.hadoopConfiguration.setLong("fs.local.block.size", 32)
    sc.hadoopConfiguration.set("io.compression.codecs",
      "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec")
    factory = new CompressionCodecFactory(sc.hadoopConfiguration)
  }

  override def afterAll() {
    sc.stop()
  }
/**
 * 创建本地文件
 */
  private def createNativeFile(inputDir: File, fileName: String, contents: Array[Byte],
                               compress: Boolean) = {
    val out = if (compress) {//是否压缩
      val codec = new GzipCodec
      val path = s"${inputDir.toString}/$fileName${codec.getDefaultExtension}"
      codec.createOutputStream(new DataOutputStream(new FileOutputStream(path)))
    } else {
      val path = s"${inputDir.toString}/$fileName"
      new DataOutputStream(new FileOutputStream(path))
    }
    out.write(contents, 0, contents.length)
    out.close()
  }

  /**
   * This code will test the behaviors of WholeTextFileRecordReader based on local disk. There are
   * 码将测试基于本地磁盘的整个文本文件记录阅读器的行为
   * three aspects(方面) to check:
   *   1) Whether all files are read;//是否全部文件读取
   *   2) Whether paths are read correctly; //是否正确路径
   *   3) Does the contents be the same.//内容是一样的吗?
   */
  test("Correctness of WholeTextFileRecordReader.") {
    val dir = Utils.createTempDir()
    logInfo(s"Local disk address is ${dir.toString}.")

    WholeTextFileRecordReaderSuite.files.foreach { case (filename, contents) =>
      createNativeFile(dir, filename, contents, false)
    }
    //一次性读取目录下的全部文件,
    val res = sc.wholeTextFiles(dir.toString, 3).collect()
    /**
     * [(file:/C:/Users/liushuhua/AppData/Local/Temp/spark-4b09702d-7878-405c-8df5-db05ce01ac6e/part-00000,Spark is e), 
     * (file:/C:/Users/liushuhua/AppData/Local/Temp/spark-4b09702d-7878-405c-8df5-db05ce01ac6e/part-00001,Spark is easy to use.
			Spark is easy to use.
      Spark is easy to use.
      Spark is easy to use.
      Spark is eas), 
      (file:/C:/Users/liushuhua/AppData/Local/Temp/spark-4b09702d-7878-405c-8df5-db05ce01ac6e/part-00002,Spark is easy to use.     
      Spark is easy to use.
      Spark is easy to use.
			Spark is e)]
     */
    assert(res.size === WholeTextFileRecordReaderSuite.fileNames.size,
      "Number of files read out does not fit with the actual value.")

    for ((filename, contents) <- res) {//
      //file:/C:/Users/liushuhua/AppData/Local/Temp/spark-4b09702d-7878-405c-8df5-db05ce01ac6e/part-00000,取出文件名
      val shortName = filename.split('/').last 
      assert(WholeTextFileRecordReaderSuite.fileNames.contains(shortName),
        s"Missing file name $filename.")
      assert(contents === new Text(WholeTextFileRecordReaderSuite.files(shortName)).toString,
        s"file $filename contents can not match.")
    }
 //递归删除
    Utils.deleteRecursively(dir)
  }

  test("Correctness of WholeTextFileRecordReader with GzipCodec.") {//读取压缩文件
    val dir = Utils.createTempDir()
    logInfo(s"Local disk address is ${dir.toString}.")

    WholeTextFileRecordReaderSuite.files.foreach { case (filename, contents) =>
      createNativeFile(dir, filename, contents, true)
    }
/**
 * [(file:/C:/Users/liushuhua/AppData/Local/Temp/spark-07f2bfa3-71cf-442a-8474-b4c8b64e61f3/part-00000.gz,Spark is e), 
 * (file:/C:/Users/liushuhua/AppData/Local/Temp/spark-07f2bfa3-71cf-442a-8474-b4c8b64e61f3/part-00001.gz,
    Spark is easy to use.
    Spark is easy to use.
    Spark is easy to use.
    Spark is easy to use.
    Spark is eas), 
	(file:/C:/Users/liushuhua/AppData/Local/Temp/spark-07f2bfa3-71cf-442a-8474-b4c8b64e61f3/part-00002.gz,
    Spark is easy to use.
    Spark is easy to use.
    Spark is easy to use.
    Spark is easy to use.
    Spark is easy to use.
    Spark is e)]
 */
    val res = sc.wholeTextFiles(dir.toString, 3).collect()

    assert(res.size === WholeTextFileRecordReaderSuite.fileNames.size,
      "Number of files read out does not fit with the actual value.")

    for ((filename, contents) <- res) {
      //part-00000.gz 取出part-00000文件名
      val shortName = filename.split('/').last.split('.')(0)

      assert(WholeTextFileRecordReaderSuite.fileNames.contains(shortName),
        s"Missing file name $filename.")
      assert(contents === new Text(WholeTextFileRecordReaderSuite.files(shortName)).toString,
        s"file $filename contents can not match.")
    }

    Utils.deleteRecursively(dir)
  }
}

/**
 * Files to be tested are defined here.
 * 要测试的文件在这里定义
 */
object WholeTextFileRecordReaderSuite {
  //testWords: IndexedSeq[Byte] = Vector(83, 112, 97, 114, 107, 32, 105, 115, 32, 101, 
  //97, 115, 121, 32, 116, 111, 32, 117, 115, 101, 46, 10)
  private val testWords: IndexedSeq[Byte] = "Spark is easy to use.\n".map(_.toByte)
  //fileNames: Array[String] = Array(part-00000, part-00001, part-00002)
  private val fileNames = Array("part-00000", "part-00001", "part-00002")
  //Array[Int] = Array(10, 100, 1000)
  private val fileLengths = Array(10, 100, 1000)
  /**
   * files: scala.collection.immutable.Map[String,Array[Byte]] = Map(part-00000 -> Ar
      ray(83, 112, 97, 114, 107, 32, 105, 115, 32, 101), part-00001 -> Array(83, 112,
      97, 114, 107, 32, 105, 115, 32, 101, 97, 115, 121, 32, 116, 111, 32, 117, 115, 1
      01, 46, 10, 83, 112, 97, 114, 107, 32, 105, 115, 32, 101, 97, 115, 121, 32, 116,
       111, 32, 117, 115, 101, 46, 10, 83, 112, 97, 114, 107, 32, 105, 115, 32, 101, 9
      7, 115, 121, 32, 116, 111, 32, 117, 115, 101, 46, 10, 83, 112, 97, 114, 107, 32,
       105, 115, 32, 101, 97, 115, 121, 32, 116, 111, 32, 117, 115, 101, 46, 10, 83, 1
      12, 97, 114, 107, 32, 105, 115, 32, 101, 97, 115), part-00002 -> Array(83, 112,
      97, 114, 107, 32, 105, 115, 32, 101, 97, 115, 121, 32, 116, 111, 32, 117, 115, 1
      01, 46, 10, 83, 112, 97, 114, 107, 32, 105, 115, 32, 101, 97, 115, 121, 32, 1...
   */
  private val files = fileLengths.zip(fileNames).map { case (upperBound, filename) =>
    //continually 不断
    filename -> Stream.continually(testWords.toList.toStream).flatten.take(upperBound).toArray
  }.toMap
}
