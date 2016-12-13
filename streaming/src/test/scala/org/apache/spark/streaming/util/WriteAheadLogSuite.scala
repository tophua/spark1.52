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

import java.io._
import java.nio.ByteBuffer
import java.util

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.Eventually._
import org.scalatest.BeforeAndAfter

import org.apache.spark.util.{ManualClock, Utils}
import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
/**
 * 提前写日志测试套件
 * WAL先写入磁盘然后写入Executor中，失败可能性不大
 */
class WriteAheadLogSuite extends SparkFunSuite with BeforeAndAfter {

  import WriteAheadLogSuite._

  val hadoopConf = new Configuration()
  var tempDir: File = null
  var testDir: String = null
  var testFile: String = null
  var writeAheadLog: FileBasedWriteAheadLog = null

  before {
    tempDir = Utils.createTempDir()
    testDir = tempDir.toString
    testFile = new File(tempDir, "testFile").toString
    if (writeAheadLog != null) {
      writeAheadLog.close()
      writeAheadLog = null
    }
  }

  after {
    Utils.deleteRecursively(tempDir)
  }

  test("WriteAheadLogUtils - log selection and creation") {//日志选择和创建
    val logDir = Utils.createTempDir().getAbsolutePath()

    def assertDriverLogClass[T <: WriteAheadLog: ClassTag](conf: SparkConf): WriteAheadLog = {
      val log = WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
      assert(log.getClass === implicitly[ClassTag[T]].runtimeClass)
      log
    }

    def assertReceiverLogClass[T: ClassTag](conf: SparkConf): WriteAheadLog = {
      val log = WriteAheadLogUtils.createLogForReceiver(conf, logDir, hadoopConf)
      assert(log.getClass === implicitly[ClassTag[T]].runtimeClass)
      log
    }

    val emptyConf = new SparkConf()  // no log configuration 没有日志配置
    assertDriverLogClass[FileBasedWriteAheadLog](emptyConf)
    assertReceiverLogClass[FileBasedWriteAheadLog](emptyConf)

    // Verify setting driver WAL class 验证设置驱动程序
    val conf1 = new SparkConf().set("spark.streaming.driver.writeAheadLog.class",
      classOf[MockWriteAheadLog0].getName())
    assertDriverLogClass[MockWriteAheadLog0](conf1)
    assertReceiverLogClass[FileBasedWriteAheadLog](conf1)

    // Verify setting receiver WAL class 验证设置接收器
    val receiverWALConf = new SparkConf().set("spark.streaming.receiver.writeAheadLog.class",
      classOf[MockWriteAheadLog0].getName())
    assertDriverLogClass[FileBasedWriteAheadLog](receiverWALConf)
    assertReceiverLogClass[MockWriteAheadLog0](receiverWALConf)

    // Verify setting receiver WAL class with 1-arg constructor
    // 验证设置接收writeAheadLog类型1个参数
    val receiverWALConf2 = new SparkConf().set("spark.streaming.receiver.writeAheadLog.class",
      classOf[MockWriteAheadLog1].getName())
    assertReceiverLogClass[MockWriteAheadLog1](receiverWALConf2)

    // Verify failure setting receiver WAL class with 2-arg constructor
    intercept[SparkException] {
      val receiverWALConf3 = new SparkConf().set("spark.streaming.receiver.writeAheadLog.class",
        classOf[MockWriteAheadLog2].getName())
      assertReceiverLogClass[MockWriteAheadLog1](receiverWALConf3)
    }
  }

  test("FileBasedWriteAheadLogWriter - writing data") {//写数据
    val dataToWrite = generateRandomData()
    val segments = writeDataUsingWriter(testFile, dataToWrite)
    val writtenData = readDataManually(segments)
    assert(writtenData === dataToWrite)
  }
  //立即同步读写数据
  test("FileBasedWriteAheadLogWriter - syncing of data by writing and reading immediately") {
    val dataToWrite = generateRandomData()
    val writer = new FileBasedWriteAheadLogWriter(testFile, hadoopConf)
    dataToWrite.foreach { data =>
      val segment = writer.write(stringToByteBuffer(data))
      val dataRead = readDataManually(Seq(segment)).head
      assert(data === dataRead)
    }
    writer.close()
  }

  test("FileBasedWriteAheadLogReader - sequentially reading data") {//顺序读取数据
    val writtenData = generateRandomData()
    writeDataManually(writtenData, testFile)
    val reader = new FileBasedWriteAheadLogReader(testFile, hadoopConf)
    val readData = reader.toSeq.map(byteBufferToString)
    assert(readData === writtenData)
    assert(reader.hasNext === false)
    intercept[Exception] {
      reader.next()
    }
    reader.close()
  }

  test("FileBasedWriteAheadLogReader - sequentially reading data written with writer") {
    //顺序读取数据和写入数据
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(testFile, dataToWrite)
    val readData = readDataUsingReader(testFile)
    assert(readData === dataToWrite)
  }
  //读写的数据与写后损坏
  test("FileBasedWriteAheadLogReader - reading data written with writer after corrupted write") {
    // Write data manually for testing the sequential reader
    //手动写入数据以测试顺序读写器
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(testFile, dataToWrite)
    val fileLength = new File(testFile).length()

    // Append some garbage data to get the effect of a corrupted write
    //添加一些垃圾数据以获取已损坏的写入的效果
    val fw = new FileWriter(testFile, true)
    fw.append("This line appended to file!")
    fw.close()

    // Verify the data can be read and is same as the one correctly written
    //验证数据可以读取,并与正确写入的数据相同
    assert(readDataUsingReader(testFile) === dataToWrite)

    // Corrupt the last correctly written file
    //损坏后的一个正确的写文件
    val raf = new FileOutputStream(testFile, true).getChannel()
    raf.truncate(fileLength - 1)
    raf.close()

    // Verify all the data except the last can be read
    //验证所有的数据,除了最新一个可以读取
    assert(readDataUsingReader(testFile) === (dataToWrite.dropRight(1)))
  }

  test("FileBasedWriteAheadLogRandomReader - reading data using random reader") {//使用随机数据读取
    // Write data manually for testing the random reader
    //手动写入数据测试随机读
    val writtenData = generateRandomData()
    val segments = writeDataManually(writtenData, testFile)

    // Get a random order of these segments and read them back
    //获取这些段的随机顺序并读取它们
    val writtenDataAndSegments = writtenData.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new FileBasedWriteAheadLogRandomReader(testFile, hadoopConf)
    writtenDataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }
  //采用随机读写读数据
  test("FileBasedWriteAheadLogRandomReader- reading data using random reader written with writer") {
    // Write data using writer for testing the random reader
    val data = generateRandomData()
    val segments = writeDataUsingWriter(testFile, data)

    // Read a random sequence of segments and verify read data
    //读取一个段的随机序列,并验证读取数据
    val dataAndSegments = data.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new FileBasedWriteAheadLogRandomReader(testFile, hadoopConf)
    dataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("FileBasedWriteAheadLog - write rotating logs") {//日志写的旋转
    // Write data with rotation using WriteAheadLog class
    val dataToWrite = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite)

    // Read data manually to verify the written data
    //手动读取数据以验证写入数据
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)
    val writtenData = logFiles.flatMap { file => readDataManually(file)}
    assert(writtenData === dataToWrite)
  }

  test("FileBasedWriteAheadLog - read rotating logs") {//读取旋转日志
    // Write data manually for testing reading through WriteAheadLog
    val writtenData = (1 to 10).map { i =>
      val data = generateRandomData()
      val file = testDir + s"/log-$i-$i"
      writeDataManually(data, file)
      data
    }.flatten

    val logDirectoryPath = new Path(testDir)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)
    assert(fileSystem.exists(logDirectoryPath) === true)

    // Read data using manager and verify
    //使用管理器读取数据并验证
    val readData = readDataUsingWriteAheadLog(testDir)
    assert(readData === writtenData)
  }
  //创建新的管理器时恢复过来的日志
  test("FileBasedWriteAheadLog - recover past logs when creating new manager") {
    // Write data with manager, recover with new manager and verify
    //管理器写入数据,新的管理器恢复并验证
    val dataToWrite = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite)
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)
    val readData = readDataUsingWriteAheadLog(testDir)
    assert(dataToWrite === readData)
  }

  test("FileBasedWriteAheadLog - clean old logs") {//清除的旧日志
    logCleanUpTest(waitForCompletion = false)
  }

  test("FileBasedWriteAheadLog - clean old logs synchronously") {//同步清理旧日志
    logCleanUpTest(waitForCompletion = true)
  }

  private def logCleanUpTest(waitForCompletion: Boolean): Unit = {
    // Write data with manager, recover with new manager and verify
    //管理器写入数据,与新的管理器恢复并验证
    val manualClock = new ManualClock
    val dataToWrite = generateRandomData()
    writeAheadLog = writeDataUsingWriteAheadLog(testDir, dataToWrite, manualClock, closeLog = false)
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)

    writeAheadLog.clean(manualClock.getTimeMillis() / 2, waitForCompletion)

    if (waitForCompletion) {
      assert(getLogFilesInDirectory(testDir).size < logFiles.size)
    } else {
      eventually(timeout(1 second), interval(10 milliseconds)) {
        assert(getLogFilesInDirectory(testDir).size < logFiles.size)
      }
    }
  }
  //读取旋转日志时处理文件错误
  test("FileBasedWriteAheadLog - handling file errors while reading rotating logs") {
    // Generate a set of log files 生成一组日志文件
    val manualClock = new ManualClock
    val dataToWrite1 = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite1, manualClock)
    val logFiles1 = getLogFilesInDirectory(testDir)
    assert(logFiles1.size > 1)


    // Recover old files and generate a second set of log files
    //恢复旧文件,并生成第二组日志文件
    val dataToWrite2 = generateRandomData()
    manualClock.advance(100000)
    writeDataUsingWriteAheadLog(testDir, dataToWrite2, manualClock)
    val logFiles2 = getLogFilesInDirectory(testDir)
    assert(logFiles2.size > logFiles1.size)

    // Read the files and verify that all the written data can be read
    //读取文件并验证所有的写入数据都可以读取
    val readData1 = readDataUsingWriteAheadLog(testDir)
    assert(readData1 === (dataToWrite1 ++ dataToWrite2))

    // Corrupt the first set of files so that they are basically unreadable
    //错误第一组的文件,它们基本上是不可读的
    logFiles1.foreach { f =>
      val raf = new FileOutputStream(f, true).getChannel()
      raf.truncate(1)
      raf.close()
    }

    // Verify that the corrupted files do not prevent reading of the second set of data
    //确认已损坏的文件不阻止第二组数据的读取
    val readData = readDataUsingWriteAheadLog(testDir)
    assert(readData === dataToWrite2)
  }
  //不要创建目录或文件,除非写
  test("FileBasedWriteAheadLog - do not create directories or files unless write") {
    val nonexistentTempPath = File.createTempFile("test", "")
    nonexistentTempPath.delete()
    assert(!nonexistentTempPath.exists())

    val writtenSegment = writeDataManually(generateRandomData(), testFile)
    val wal = new FileBasedWriteAheadLog(
      new SparkConf(), tempDir.getAbsolutePath, new Configuration(), 1, 1)
    assert(!nonexistentTempPath.exists(), "Directory created just by creating log object")
    wal.read(writtenSegment.head)
    assert(!nonexistentTempPath.exists(), "Directory created just by attempting to read segment")
  }
}

object WriteAheadLogSuite {

  class MockWriteAheadLog0() extends WriteAheadLog {
    override def write(record: ByteBuffer, time: Long): WriteAheadLogRecordHandle = { null }
    override def read(handle: WriteAheadLogRecordHandle): ByteBuffer = { null }
    override def readAll(): util.Iterator[ByteBuffer] = { null }
    override def clean(threshTime: Long, waitForCompletion: Boolean): Unit = { }
    override def close(): Unit = { }
  }

  class MockWriteAheadLog1(val conf: SparkConf) extends MockWriteAheadLog0()

  class MockWriteAheadLog2(val conf: SparkConf, x: Int) extends MockWriteAheadLog0()


  private val hadoopConf = new Configuration()

  /** 
   *  Write data to a file directly and return an array of the file segments written.
   *  直接将数据写入文件并返回写入的文件段的数组
   *   */
  def writeDataManually(data: Seq[String], file: String): Seq[FileBasedWriteAheadLogSegment] = {
    val segments = new ArrayBuffer[FileBasedWriteAheadLogSegment]()
    //获得val writer: FSDataOutputStream
    val writer = HdfsUtils.getOutputStream(file, hadoopConf)
    data.foreach { item =>
      val offset = writer.getPos
      val bytes = Utils.serialize(item)
      writer.writeInt(bytes.size)
      writer.write(bytes)
      segments += FileBasedWriteAheadLogSegment(file, offset, bytes.size)
    }
    writer.close()
    segments
  }

  /**
   * Write data to a file using the writer class and return an array of the file segments written.
   * 使用“写入”类将数据写入文件，并返回写入的文件段的数组
   */
  def writeDataUsingWriter(
      filePath: String,
      data: Seq[String]
    ): Seq[FileBasedWriteAheadLogSegment] = {
    val writer = new FileBasedWriteAheadLogWriter(filePath, hadoopConf)
    val segments = data.map {
      item => writer.write(item)
    }
    writer.close()
    segments
  }

  /** 
   *  Write data to rotating files in log directory using the WriteAheadLog class.
   *  写数据到旋转文件使用writeaheadlog班日志目录
   *   */
  def writeDataUsingWriteAheadLog(
      logDirectory: String,
      data: Seq[String],
      manualClock: ManualClock = new ManualClock,
      closeLog: Boolean = true
    ): FileBasedWriteAheadLog = {
    if (manualClock.getTimeMillis() < 100000) manualClock.setTime(10000)
    val wal = new FileBasedWriteAheadLog(new SparkConf(), logDirectory, hadoopConf, 1, 1)

    // Ensure that 500 does not get sorted after 2000, so put a high base value.
    //确保500不被排序后的2000，所以放一个高的基础值。
    data.foreach { item =>
      manualClock.advance(500)
      wal.write(item, manualClock.getTimeMillis())
    }
    if (closeLog) wal.close()
    wal
  }

  /** 
   *  Read data from a segments of a log file directly and return the list of byte buffers.
   *  直接从日志文件中读取数据,并返回字节缓冲区的列表
   *   */
  def readDataManually(segments: Seq[FileBasedWriteAheadLogSegment]): Seq[String] = {
    segments.map { segment =>
      val reader = HdfsUtils.getInputStream(segment.path, hadoopConf)
      try {
        reader.seek(segment.offset)
        val bytes = new Array[Byte](segment.length)
        reader.readInt()
        reader.readFully(bytes)
        val data = Utils.deserialize[String](bytes)
        reader.close()
        data
      } finally {
        reader.close()
      }
    }
  }

  /** 
   *  Read all the data from a log file directly and return the list of byte buffers.
   *  直接从日志文件中读取所有的数据,并返回字节缓冲区的列表 
   *  */
  def readDataManually(file: String): Seq[String] = {
    val reader = HdfsUtils.getInputStream(file, hadoopConf)
    val buffer = new ArrayBuffer[String]
    try {
      while (true) {
        // Read till EOF is thrown
        val length = reader.readInt()
        val bytes = new Array[Byte](length)
        reader.read(bytes)
        buffer += Utils.deserialize[String](bytes)
      }
    } catch {
      case ex: EOFException =>
    } finally {
      reader.close()
    }
    buffer
  }

  /** 
   *  Read all the data from a log file using reader class and return the list of byte buffers.
   *  从使用读类器读取日志文件中的所有数据,并返回字节缓冲区的列表
   *   */
  def readDataUsingReader(file: String): Seq[String] = {
    val reader = new FileBasedWriteAheadLogReader(file, hadoopConf)
    val readData = reader.toList.map(byteBufferToString)
    reader.close()
    readData
  }

  /** 
   *  Read all the data in the log file in a directory using the WriteAheadLog class.
   *  读取writeaheadlog类目录中的日志文件中的所有数据
   *   */
  def readDataUsingWriteAheadLog(logDirectory: String): Seq[String] = {
    import scala.collection.JavaConversions._
    val wal = new FileBasedWriteAheadLog(new SparkConf(), logDirectory, hadoopConf, 1, 1)
    val data = wal.readAll().map(byteBufferToString).toSeq
    wal.close()
    data
  }

  /** 
   *  Get the log files in a direction
   *  获取方目录的日志文件
   *   */
  def getLogFilesInDirectory(directory: String): Seq[String] = {
    val logDirectoryPath = new Path(directory)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)

    if (fileSystem.exists(logDirectoryPath) && fileSystem.getFileStatus(logDirectoryPath).isDir) {
      fileSystem.listStatus(logDirectoryPath).map { _.getPath() }.sortBy {
        _.getName().split("-")(1).toLong
      }.map {
        _.toString.stripPrefix("file:")
      }
    } else {
      Seq.empty
    }
  }

  def generateRandomData(): Seq[String] = {
    (1 to 100).map { _.toString }
  }

  implicit def stringToByteBuffer(str: String): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize(str))
  }

  implicit def byteBufferToString(byteBuffer: ByteBuffer): String = {
    Utils.deserialize[String](byteBuffer.array)
  }
}
