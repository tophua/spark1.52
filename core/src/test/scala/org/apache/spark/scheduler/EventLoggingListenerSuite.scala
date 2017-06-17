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

package org.apache.spark.scheduler

import java.io.{File, FileOutputStream, InputStream, IOException}
import java.net.URI

import scala.collection.mutable
import scala.io.Source

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io._
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * Test whether EventLoggingListener logs events properly.
 * 测试是否eventlogginglistener日志事件属性
 *
 * This tests whether EventLoggingListener actually log files with expected name patterns while
 * 是否实际eventlogginglistener日志名称与期望的日志事件名称模式相匹配
 * logging events, whether the parsing of the file names is correct, and whether the logged events
 * 解析文件名是否正确
 * can be read and deserialized into actual SparkListenerEvents.
 */
class EventLoggingListenerSuite extends SparkFunSuite with LocalSparkContext with BeforeAndAfter
  with Logging {
  import EventLoggingListenerSuite._

  private val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  private var testDir: File = _
  private var testDirPath: Path = _

  before {
    testDir = Utils.createTempDir()
    testDir.deleteOnExit()
    testDirPath = new Path(testDir.getAbsolutePath())
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  test("Verify log file exist") {//验证日志文件存在
    // Verify logging directory exists
    //检查日志记录目录是否存在
    val conf = getLoggingConf(testDirPath)
    val eventLogger = new EventLoggingListener("test", None, testDirPath.toUri(), conf)
    eventLogger.start()

    val logPath = new Path(eventLogger.logPath + EventLoggingListener.IN_PROGRESS)
    assert(fileSystem.exists(logPath))
    val logStatus = fileSystem.getFileStatus(logPath)
    assert(!logStatus.isDir)

    // Verify log is renamed after stop()
    //验证日志重命名后stop()
    eventLogger.stop()
    assert(!fileSystem.getFileStatus(new Path(eventLogger.logPath)).isDir)
  }

  test("Basic event logging") {//基本事件日志
    testEventLogging()
  }

  test("Basic event logging with compression") {//压缩基本事件日志记录
    CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codec =>
      testEventLogging(compressionCodec = Some(CompressionCodec.getShortName(codec)))
    }
  }

  test("End-to-end event logging") {//端到端事件日志记录
    //testApplicationEventLogging()
  }

  test("End-to-end event logging with compression") {//端到端的压缩事件日志记录
   /* CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codec =>
      testApplicationEventLogging(compressionCodec = Some(CompressionCodec.getShortName(codec)))
    }*/
  }

  test("Log overwriting") {//日志覆盖
    val logUri = EventLoggingListener.getLogPath(testDir.toURI, "test", None)
    val logPath = new URI(logUri).getPath
    // Create file before writing the event log
    // 在写入事件日志之前创建文件
    new FileOutputStream(new File(logPath)).close()
    // Expected IOException, since we haven't enabled log overwrite.
    // 异常IOException,由于我们还没有启用日志覆盖
    intercept[IOException] { testEventLogging() }
    // Try again, but enable overwriting.
    //再试一次,但使覆盖
    testEventLogging(extraConf = Map("spark.eventLog.overwrite" -> "true"))
  }

  test("Event log name") {//事件日志名称
    // without compression 无压缩
    assert(s"file:/base-dir/app1" === EventLoggingListener.getLogPath(
      Utils.resolveURI("/base-dir"), "app1", None))
    // with compression 压缩
    assert(s"file:/base-dir/app1.lzf" ===
      EventLoggingListener.getLogPath(Utils.resolveURI("/base-dir"), "app1", None, Some("lzf")))
    // illegal characters in app ID 应用程序中的非法字符
    assert(s"file:/base-dir/a-fine-mind_dollar_bills__1" ===
      EventLoggingListener.getLogPath(Utils.resolveURI("/base-dir"),
        "a fine:mind$dollar{bills}.1", None))
    // illegal characters in app ID with compression
    //具有压缩的应用程序中的非法字符
    assert(s"file:/base-dir/a-fine-mind_dollar_bills__1.lz4" ===
      EventLoggingListener.getLogPath(Utils.resolveURI("/base-dir"),
        "a fine:mind$dollar{bills}.1", None, Some("lz4")))
  }

  /* ----------------- *
   * Actual test logic *
   * ----------------- */

  import EventLoggingListenerSuite._

  /**
   * Test basic event logging functionality.
   * 测试基本事件日志记录功能
   * This creates two simple events, posts them to the EventLoggingListener, and verifies that
   * 这创造了两个简单的事件,提交到EventLoggingListener,并验证这两个事件是否在预期的文件中记录
   * exactly these two events are logged in the expected file.
   */
  private def testEventLogging(
      compressionCodec: Option[String] = None,
      extraConf: Map[String, String] = Map()) {
    val conf = getLoggingConf(testDirPath, compressionCodec)
    extraConf.foreach { case (k, v) => conf.set(k, v) }
    val logName = compressionCodec.map("test-" + _).getOrElse("test")
    val eventLogger = new EventLoggingListener(logName, None, testDirPath.toUri(), conf)
    val listenerBus = new LiveListenerBus
    val applicationStart = SparkListenerApplicationStart("Greatest App (N)ever", None,
      125L, "Mickey", None)
    val applicationEnd = SparkListenerApplicationEnd(1000L)

    // A comprehensive test on JSON de/serialization of all events is in JsonProtocolSuite
    //一个综合测试JSON序列化/反列化,所有的事件是jsonprotocolsuite
    eventLogger.start()
    listenerBus.start(sc)
    listenerBus.addListener(eventLogger)
    listenerBus.postToAll(applicationStart)
    listenerBus.postToAll(applicationEnd)
    eventLogger.stop()

    // Verify file contains exactly the two events logged
    //验证文件中包含的正是两个事件记录的
    val logData = EventLoggingListener.openEventLog(new Path(eventLogger.logPath), fileSystem)
    try {
      val lines = readLines(logData)
      val logStart = SparkListenerLogStart(SPARK_VERSION)
      assert(lines.size === 3)
      assert(lines(0).contains("SparkListenerLogStart"))
      assert(lines(1).contains("SparkListenerApplicationStart"))
      assert(lines(2).contains("SparkListenerApplicationEnd"))
      assert(JsonProtocol.sparkEventFromJson(parse(lines(0))) === logStart)
      assert(JsonProtocol.sparkEventFromJson(parse(lines(1))) === applicationStart)
      assert(JsonProtocol.sparkEventFromJson(parse(lines(2))) === applicationEnd)
    } finally {
      logData.close()
    }
  }

  /**
   * Test end-to-end event logging functionality in an application.
   * 在应用程序中测试端到端的事件日志记录功能
   * This runs a simple Spark job and asserts that the expected events are logged when expected.
   */
  private def testApplicationEventLogging(compressionCodec: Option[String] = None) {
    // Set defaultFS to something that would cause an exception, to make sure we don't run
    // into SPARK-6688.
    val conf = getLoggingConf(testDirPath, compressionCodec)
      .set("spark.hadoop.fs.defaultFS", "unsupported://example.com")
    val sc = new SparkContext("local-cluster[2,2,1024]", "test", conf)
    assert(sc.eventLogger.isDefined)
    val eventLogger = sc.eventLogger.get
    val eventLogPath = eventLogger.logPath
    val expectedLogDir = testDir.toURI()
    assert(eventLogPath === EventLoggingListener.getLogPath(
      expectedLogDir, sc.applicationId, None, compressionCodec.map(CompressionCodec.getShortName)))

    // Begin listening for events that trigger asserts
    //开始监听触发断言的事件
    val eventExistenceListener = new EventExistenceListener(eventLogger)
    sc.addSparkListener(eventExistenceListener)

    // Trigger asserts for whether the expected events are actually logged
    //触发器断言是否实际记录的预期事件
    sc.parallelize(1 to 10000).count()
    sc.stop()

    // Ensure all asserts have actually been triggered
    //确保所有的断言实际上已经被触发
    eventExistenceListener.assertAllCallbacksInvoked()

    // Make sure expected events exist in the log file.
    //确保日志文件中存在预期的事件
    val logData = EventLoggingListener.openEventLog(new Path(eventLogger.logPath), fileSystem)
    val logStart = SparkListenerLogStart(SPARK_VERSION)
    val lines = readLines(logData)
    val eventSet = mutable.Set(
      SparkListenerApplicationStart,
      SparkListenerBlockManagerAdded,
      SparkListenerExecutorAdded,
      SparkListenerEnvironmentUpdate,
      SparkListenerJobStart,
      SparkListenerJobEnd,
      SparkListenerStageSubmitted,
      SparkListenerStageCompleted,
      SparkListenerTaskStart,
      SparkListenerTaskEnd,
      SparkListenerApplicationEnd).map(Utils.getFormattedClassName)
    lines.foreach { line =>
      eventSet.foreach { event =>
        if (line.contains(event)) {
          val parsedEvent = JsonProtocol.sparkEventFromJson(parse(line))
          val eventType = Utils.getFormattedClassName(parsedEvent)
          if (eventType == event) {
            eventSet.remove(event)
          }
        }
      }
    }
    assert(JsonProtocol.sparkEventFromJson(parse(lines(0))) === logStart)
    assert(eventSet.isEmpty, "The following events are missing: " + eventSet.toSeq)
  }

  private def readLines(in: InputStream): Seq[String] = {
    Source.fromInputStream(in).getLines().toSeq
  }

  /**
   * A listener that asserts certain events are logged by the given EventLoggingListener.
   * This is necessary because events are posted asynchronously in a different thread.
   */
  private class EventExistenceListener(eventLogger: EventLoggingListener) extends SparkListener {
    var jobStarted = false
    var jobEnded = false
    var appEnded = false

    override def onJobStart(jobStart: SparkListenerJobStart) {
      jobStarted = true
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd) {
      jobEnded = true
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
      appEnded = true
    }

    def assertAllCallbacksInvoked() {
      assert(jobStarted, "JobStart callback not invoked!")
      assert(jobEnded, "JobEnd callback not invoked!")
      assert(appEnded, "ApplicationEnd callback not invoked!")
    }
  }

}


object EventLoggingListenerSuite {

  /** 
   *  Get a SparkConf with event logging enabled.
   *  得到一个sparkconf事件启用日志记录
   *   */
  def getLoggingConf(logDir: Path, compressionCodec: Option[String] = None): SparkConf = {
    val conf = new SparkConf
    //是否记录Spark事件,用于应用程序在完成后重构webUI
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.testing", "true")
    //如果spark.eventLog.enabled为 true,该属性为记录spark事件的根目录,
    //在此根目录中,Spark为每个应用程序创建分目录,并将应用程序的事件记录到在此目录中
    conf.set("spark.eventLog.dir", logDir.toString)
    //是否压缩记录Spark事件,前提spark.eventLog.enabled为true
    compressionCodec.foreach { codec =>
      conf.set("spark.eventLog.compress", "true")
      //用于压缩内部数据如 RDD分区和shuffle输出的编码解码器
      conf.set("spark.io.compression.codec", codec)
    }
    conf
  }

  def getUniqueApplicationId: String = "test-" + System.currentTimeMillis
}
