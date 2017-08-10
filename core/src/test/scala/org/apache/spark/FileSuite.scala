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

package org.apache.spark

import java.io.{File, FileWriter}

import org.apache.spark.input.PortableDataStream
import org.apache.spark.storage.StorageLevel

import scala.io.Source

import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapred.{JobConf, FileAlreadyExistsException, FileSplit, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileSplit => NewFileSplit, TextInputFormat => NewTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}

import org.apache.spark.rdd.{NewHadoopRDD, HadoopRDD}
import org.apache.spark.util.Utils

class FileSuite extends SparkFunSuite with LocalSparkContext {
  var tempDir: File = _

  override def beforeEach() {
    super.beforeEach()
    tempDir = Utils.createTempDir()
  }

  override def afterEach() {
    super.afterEach()
      Utils.deleteRecursively(tempDir)
  }

  test("text files") {//文本文件
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath//返回抽象路径名的绝对路径名字符串
    println("==="+outputDir);
    val nums = sc.makeRDD(1 to 4)
    nums.saveAsTextFile(outputDir)//保存文件1,2,3,4
    // Read the plain text file and check it's OK
    //读简单的文本文件,并检查它的正确
    val outputFile = new File(outputDir, "part-00000")//父目录,子目录
    val content = Source.fromFile(outputFile).mkString
    println("content:"+content);
    assert(content === "1\n2\n3\n4\n")
    // Also try reading it in as a text file RDD
    //也试着读它为文本文件RDD
    assert(sc.textFile(outputDir).collect().toList === List("1", "2", "3", "4"))
  }

  //文本文件压缩
  test("text files (compressed)") {
    
    sc = new SparkContext("local", "test")
    val normalDir = new File(tempDir, "output_normal").getAbsolutePath
    val compressedOutputDir = new File(tempDir, "output_compressed").getAbsolutePath
    val codec = new DefaultCodec()

    val data = sc.parallelize("a" * 10000, 1)
    data.saveAsTextFile(normalDir)
    data.saveAsTextFile(compressedOutputDir, classOf[DefaultCodec])

    val normalFile = new File(normalDir, "part-00000")
    val normalContent = sc.textFile(normalDir).collect
    assert(normalContent === Array.fill(10000)("a"))

    val compressedFile = new File(compressedOutputDir, "part-00000" + codec.getDefaultExtension)
    val compressedContent = sc.textFile(compressedOutputDir).collect
    assert(compressedContent === Array.fill(10000)("a"))

    assert(compressedFile.length < normalFile.length)
  }

  test("SequenceFiles") {//序列文件
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x)) // (1,a), (2,aa), (3,aaa)
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    //尝试读取输出到序列文件
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("SequenceFile (compressed)") {//序列文件压缩
    sc = new SparkContext("local", "test")
    val normalDir = new File(tempDir, "output_normal").getAbsolutePath
    val compressedOutputDir = new File(tempDir, "output_compressed").getAbsolutePath
    val codec = new DefaultCodec()

    val data = sc.parallelize(Seq.fill(100)("abc"), 1).map(x => (x, x))
    data.saveAsSequenceFile(normalDir)
    data.saveAsSequenceFile(compressedOutputDir, Some(classOf[DefaultCodec]))

    val normalFile = new File(normalDir, "part-00000")
    val normalContent = sc.sequenceFile[String, String](normalDir).collect
    assert(normalContent === Array.fill(100)("abc", "abc"))

    val compressedFile = new File(compressedOutputDir, "part-00000" + codec.getDefaultExtension)
    val compressedContent = sc.sequenceFile[String, String](compressedOutputDir).collect
    assert(compressedContent === Array.fill(100)("abc", "abc"))

    assert(compressedFile.length < normalFile.length)
  }

  test("SequenceFile with writable key") {//可写Key的序列文件
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), "a" * x))
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("SequenceFile with writable value") {//可写value的序列文件
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("SequenceFile with writable key and value") {//可写Key和value的序列文件
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("implicit conversions in reading SequenceFiles") {//隐式转换读取序列文件
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x)) // (1,a), (2,aa), (3,aaa)
    nums.saveAsSequenceFile(outputDir)
    // Similar to the tests above, we read a SequenceFile, but this time we pass type params
    // that are convertable to Writable instead of calling sequenceFile[IntWritable, Text]
    //类似以上的测试,读了一个序列文件
    val output1 = sc.sequenceFile[Int, String](outputDir)
    assert(output1.collect().toList === List((1, "a"), (2, "aa"), (3, "aaa")))
    // Also try having one type be a subclass of Writable and one not
    //还有一种是可写的,一个没有子类
    val output2 = sc.sequenceFile[Int, Text](outputDir)
    assert(output2.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    val output3 = sc.sequenceFile[IntWritable, String](outputDir)
    assert(output3.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }

  test("object files of ints") {//Int类型对象文件
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 4)
    nums.saveAsObjectFile(outputDir)
    // Try reading the output back as an object file
    //尝试读取输出作为一个对象文件
    val output = sc.objectFile[Int](outputDir)
    assert(output.collect().toList === List(1, 2, 3, 4))
  }

  test("object files of complex types") {//复杂类型的对象文件
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x))
    nums.saveAsObjectFile(outputDir)
    // Try reading the output back as an object file
    //尝试读取输出作为一个对象文件
    val output = sc.objectFile[(Int, String)](outputDir)
    assert(output.collect().toList === List((1, "a"), (2, "aa"), (3, "aaa")))
  }

  test("object files of classes from a JAR") {//从一个Jar中的类对象文件
    // scalastyle:off classforname
    val original = Thread.currentThread().getContextClassLoader
    val className = "FileSuiteObjectFileTest"
    val jar = TestUtils.createJarWithClasses(Seq(className))
    val loader = new java.net.URLClassLoader(Array(jar), Utils.getContextOrSparkClassLoader)
    Thread.currentThread().setContextClassLoader(loader)
    try {
      sc = new SparkContext("local", "test")
      val objs = sc.makeRDD(1 to 3).map { x =>
        val loader = Thread.currentThread().getContextClassLoader
        Class.forName(className, true, loader).newInstance()
      }
      val outputDir = new File(tempDir, "output").getAbsolutePath
      objs.saveAsObjectFile(outputDir)
      // Try reading the output back as an object file
      val ct = reflect.ClassTag[Any](Class.forName(className, true, loader))
      val output = sc.objectFile[Any](outputDir)
      assert(output.collect().size === 3)
      assert(output.collect().head.getClass.getName === className)
    }
    finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    // scalastyle:on classforname
  }

  test("write SequenceFile using new Hadoop API") {//使用新的Hadoop API写入序列文件
    import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsNewAPIHadoopFile[SequenceFileOutputFormat[IntWritable, Text]](
        outputDir)
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }
  //使用新的Hadoop API读取序列文件
  test("read SequenceFile using new Hadoop API") {
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
    sc = new SparkContext("local", "test")
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    val output =
      sc.newAPIHadoopFile[IntWritable, Text, SequenceFileInputFormat[IntWritable, Text]](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
  }
  //二进制文件输入作为字节数组
  test("binary file input as byte array") {
    sc = new SparkContext("local", "test")
    val outFile = new File(tempDir, "record-bytestream-00000.bin")
    val outFileName = outFile.getAbsolutePath()

    // create file
    //创建文件
    val testOutput = Array[Byte](1, 2, 3, 4, 5, 6)
    val bbuf = java.nio.ByteBuffer.wrap(testOutput)
    // write data to file
    //写入数据到文件
    val file = new java.io.FileOutputStream(outFile)
    val channel = file.getChannel
    channel.write(bbuf)
    channel.close()
    file.close()

    val inRdd = sc.binaryFiles(outFileName)
    val (infile: String, indata: PortableDataStream) = inRdd.collect.head

    // Make sure the name and array match
    //确保名称和数组匹配
    assert(infile.contains(outFileName)) // a prefix may get added 前缀可以添加
    assert(indata.toArray === testOutput)
  }

  test("portabledatastream caching tests") {//测试缓存数据流
    sc = new SparkContext("local", "test")
    val outFile = new File(tempDir, "record-bytestream-00000.bin")
    val outFileName = outFile.getAbsolutePath()

    // create file 创建文件
    val testOutput = Array[Byte](1, 2, 3, 4, 5, 6)
    val bbuf = java.nio.ByteBuffer.wrap(testOutput)
    // write data to file 写入数据到文件
    val file = new java.io.FileOutputStream(outFile)
    //FileChannel是一个连接到文件的通道，可以通过文件通道读写文件。它无法设置为非阻塞模式，总是运行在阻塞模式下
    val channel = file.getChannel
    //channel.lock()
    //channel.lock()
    channel.write(bbuf)
    channel.close()
    file.close()

    val inRdd = sc.binaryFiles(outFileName).cache()
    inRdd.foreach{
      curData: (String, PortableDataStream) =>
       curData._2.toArray() // force the file to read 强制读取的文件
    }
    val mappedRdd = inRdd.map {
      curData: (String, PortableDataStream) =>
        (curData._2.getPath(), curData._2)
    }
    val (infile: String, indata: PortableDataStream) = mappedRdd.collect.head

    // Try reading the output back as an object file
    //尝试读取输出作为一个对象文件

    assert(indata.toArray === testOutput)
  }
  //持久化磁盘存储
  test("portabledatastream persist disk storage") {
    sc = new SparkContext("local", "test")
    val outFile = new File(tempDir, "record-bytestream-00000.bin")
    val outFileName = outFile.getAbsolutePath()

    // create file 创建文件
    val testOutput = Array[Byte](1, 2, 3, 4, 5, 6)
    val bbuf = java.nio.ByteBuffer.wrap(testOutput)
    // write data to file 写入数据到文件
    val file = new java.io.FileOutputStream(outFile)
    val channel = file.getChannel
    channel.write(bbuf)
    channel.close()
    file.close()

    val inRdd = sc.binaryFiles(outFileName).persist(StorageLevel.DISK_ONLY)
    inRdd.foreach{
      curData: (String, PortableDataStream) =>
        curData._2.toArray() // force the file to read 强制读取的文件
    }
    val mappedRdd = inRdd.map {
      curData: (String, PortableDataStream) =>
        (curData._2.getPath(), curData._2)
    }
    val (infile: String, indata: PortableDataStream) = mappedRdd.collect.head

    // Try reading the output back as an object file
    //尝试读取输出作为一个对象文件
    assert(indata.toArray === testOutput)
  }

  test("portabledatastream flatmap tests") {
    sc = new SparkContext("local", "test")
    val outFile = new File(tempDir, "record-bytestream-00000.bin")
    val outFileName = outFile.getAbsolutePath()

    // create file 创建文件
    val testOutput = Array[Byte](1, 2, 3, 4, 5, 6)
    val numOfCopies = 3
    val bbuf = java.nio.ByteBuffer.wrap(testOutput)
    // write data to file
    //写入数据到文件
    val file = new java.io.FileOutputStream(outFile)
    val channel = file.getChannel
    channel.write(bbuf)
    channel.close()
    file.close()

    val inRdd = sc.binaryFiles(outFileName)
    val mappedRdd = inRdd.map {
      curData: (String, PortableDataStream) =>
        (curData._2.getPath(), curData._2)
    }
    val copyRdd = mappedRdd.flatMap {
      curData: (String, PortableDataStream) =>
        for (i <- 1 to numOfCopies) yield (i, curData._2)
    }

    val copyArr: Array[(Int, PortableDataStream)] = copyRdd.collect()

    // Try reading the output back as an object file
    //尝试读取输出作为一个对象文件
    assert(copyArr.length == numOfCopies)
    copyArr.foreach{
      cEntry: (Int, PortableDataStream) =>
        assert(cEntry._2.toArray === testOutput)
    }

  }
  //固定记录长度二进制文件作为字节数组
  test("fixed record length binary file as byte array") {
    // a fixed length of 6 bytes
    //一个固定长度的6字节
    sc = new SparkContext("local", "test")

    val outFile = new File(tempDir, "record-bytestream-00000.bin")
    val outFileName = outFile.getAbsolutePath()

    // create file 创建文件
    val testOutput = Array[Byte](1, 2, 3, 4, 5, 6)
    val testOutputCopies = 10

    // write data to file 写入数据到文件
    val file = new java.io.FileOutputStream(outFile)
    val channel = file.getChannel
    for(i <- 1 to testOutputCopies) {
      val bbuf = java.nio.ByteBuffer.wrap(testOutput)
      channel.write(bbuf)
    }
    channel.close()
    file.close()

    val inRdd = sc.binaryRecords(outFileName, testOutput.length)
    // make sure there are enough elements
    //确保有足够的元素
    assert(inRdd.count == testOutputCopies)

    // now just compare the first one
    //现在只比较第一个
    val indata: Array[Byte] = inRdd.collect.head
    assert(indata === testOutput)
  }
  //负二进制记录长度应引发异常
  test ("negative binary record length should raise an exception") {
    // a fixed length of 6 bytes
    //一个固定长度的6字节
    sc = new SparkContext("local", "test")

    val outFile = new File(tempDir, "record-bytestream-00000.bin")
    val outFileName = outFile.getAbsolutePath()

    // create file 创建文件
    val testOutput = Array[Byte](1, 2, 3, 4, 5, 6)
    val testOutputCopies = 10

    // write data to file 写入数据到文件
    val file = new java.io.FileOutputStream(outFile)
    val channel = file.getChannel
    for(i <- 1 to testOutputCopies) {
      val bbuf = java.nio.ByteBuffer.wrap(testOutput)
      channel.write(bbuf)
    }
    channel.close()
    file.close()

    val inRdd = sc.binaryRecords(outFileName, -1)

    intercept[SparkException] {
      inRdd.count
    }
  }

  test("file caching") {//文件缓存
    sc = new SparkContext("local", "test")
    val out = new FileWriter(tempDir + "/input")
    out.write("Hello world!\n")
    out.write("What's up?\n")
    out.write("Goodbye\n")
    out.close()
    val rdd = sc.textFile(tempDir + "/input").cache()
    rdd.foreach(println _)
    assert(rdd.count() === 3)
    assert(rdd.count() === 3)
    assert(rdd.count() === 3)
  }
  //防止用户覆盖空目录
  test ("prevent user from overwriting the empty directory (old Hadoop API)") {
    sc = new SparkContext("local", "test")
    val randomRDD = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c")), 1)
    intercept[FileAlreadyExistsException] {
      randomRDD.saveAsTextFile(tempDir.getPath)
    }
  }
  //防止用户覆盖非空的目录
  test ("prevent user from overwriting the non-empty directory (old Hadoop API)") {
    sc = new SparkContext("local", "test")
    val randomRDD = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c")), 1)
    randomRDD.saveAsTextFile(tempDir.getPath + "/output")
    assert(new File(tempDir.getPath + "/output/part-00000").exists() === true)
    intercept[FileAlreadyExistsException] {
      randomRDD.saveAsTextFile(tempDir.getPath + "/output")
    }
  }
 //允许用户禁用输出目录存在检查
  test ("allow user to disable the output directory existence checking (old Hadoop API") {
    val sf = new SparkConf()
    sf.setAppName("test").setMaster("local").set("spark.hadoop.validateOutputSpecs", "false")
    sc = new SparkContext(sf)
    val randomRDD = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c")), 1)
    randomRDD.saveAsTextFile(tempDir.getPath + "/output")
    assert(new File(tempDir.getPath + "/output/part-00000").exists() === true)
    randomRDD.saveAsTextFile(tempDir.getPath + "/output")
    assert(new File(tempDir.getPath + "/output/part-00000").exists() === true)
  }
  //防止用户覆盖空目录
  test ("prevent user from overwriting the empty directory (new Hadoop API)") {
    sc = new SparkContext("local", "test")
    val randomRDD = sc.parallelize(
      Array(("key1", "a"), ("key2", "a"), ("key3", "b"), ("key4", "c")), 1)
    intercept[FileAlreadyExistsException] {
      randomRDD.saveAsNewAPIHadoopFile[NewTextOutputFormat[String, String]](tempDir.getPath)
    }
  }
  //防止用户覆盖非空的目录
  test ("prevent user from overwriting the non-empty directory (new Hadoop API)") {
    sc = new SparkContext("local", "test")
    val randomRDD = sc.parallelize(
      Array(("key1", "a"), ("key2", "a"), ("key3", "b"), ("key4", "c")), 1)
    randomRDD.saveAsNewAPIHadoopFile[NewTextOutputFormat[String, String]](
      tempDir.getPath + "/output")
    assert(new File(tempDir.getPath + "/output/part-r-00000").exists() === true)
    intercept[FileAlreadyExistsException] {
      randomRDD.saveAsNewAPIHadoopFile[NewTextOutputFormat[String, String]](tempDir.getPath)
    }
  }
  //允许用户禁用输出目录存在检查
  test ("allow user to disable the output directory existence checking (new Hadoop API") {
    val sf = new SparkConf()
    sf.setAppName("test").setMaster("local").set("spark.hadoop.validateOutputSpecs", "false")
    sc = new SparkContext(sf)
    val randomRDD = sc.parallelize(
      Array(("key1", "a"), ("key2", "a"), ("key3", "b"), ("key4", "c")), 1)
    randomRDD.saveAsNewAPIHadoopFile[NewTextOutputFormat[String, String]](
      tempDir.getPath + "/output")
    assert(new File(tempDir.getPath + "/output/part-r-00000").exists() === true)
    randomRDD.saveAsNewAPIHadoopFile[NewTextOutputFormat[String, String]](
      tempDir.getPath + "/output")
    assert(new File(tempDir.getPath + "/output/part-r-00000").exists() === true)
  }
  //通过旧的Hadoop API保存Hadoop数据集
  test ("save Hadoop Dataset through old Hadoop API") {
    sc = new SparkContext("local", "test")
    val randomRDD = sc.parallelize(
      Array(("key1", "a"), ("key2", "a"), ("key3", "b"), ("key4", "c")), 1)
    val job = new JobConf()
    job.setOutputKeyClass(classOf[String])
    job.setOutputValueClass(classOf[String])
    job.set("mapred.output.format.class", classOf[TextOutputFormat[String, String]].getName)
    job.set("mapred.output.dir", tempDir.getPath + "/outputDataset_old")
    randomRDD.saveAsHadoopDataset(job)
    assert(new File(tempDir.getPath + "/outputDataset_old/part-00000").exists() === true)
  }
  //通过新的Hadoop API保存Hadoop数据集
  test ("save Hadoop Dataset through new Hadoop API") {
    sc = new SparkContext("local", "test")
    val randomRDD = sc.parallelize(
      Array(("key1", "a"), ("key2", "a"), ("key3", "b"), ("key4", "c")), 1)
    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[String])
    job.setOutputValueClass(classOf[String])
    job.setOutputFormatClass(classOf[NewTextOutputFormat[String, String]])
    job.getConfiguration.set("mapred.output.dir", tempDir.getPath + "/outputDataset_new")
    randomRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    assert(new File(tempDir.getPath + "/outputDataset_new/part-r-00000").exists() === true)
  }

 test("Get input files via old Hadoop API") {
    sc = new SparkContext("local", "test")
    val outDir = new File(tempDir, "output").getAbsolutePath
    sc.makeRDD(1 to 4, 2).saveAsTextFile(outDir)

    val inputPaths =
      sc.hadoopFile(outDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
        .asInstanceOf[HadoopRDD[_, _]]
        .mapPartitionsWithInputSplit { (split, part) =>
          Iterator(split.asInstanceOf[FileSplit].getPath.toUri.getPath)
        }.collect()
    assert(inputPaths.toSet === Set(s"$outDir/part-00000", s"$outDir/part-00001"))
  }

  test("Get input files via new Hadoop API") {
    sc = new SparkContext("local", "test")
    val outDir = new File(tempDir, "output").getAbsolutePath
    sc.makeRDD(1 to 4, 2).saveAsTextFile(outDir)

    val inputPaths =
      sc.newAPIHadoopFile(outDir, classOf[NewTextInputFormat], classOf[LongWritable], classOf[Text])
        .asInstanceOf[NewHadoopRDD[_, _]]
        .mapPartitionsWithInputSplit { (split, part) =>
          Iterator(split.asInstanceOf[NewFileSplit].getPath.toUri.getPath)
        }.collect()
    assert(inputPaths.toSet === Set(s"$outDir/part-00000", s"$outDir/part-00001"))
  }
}
