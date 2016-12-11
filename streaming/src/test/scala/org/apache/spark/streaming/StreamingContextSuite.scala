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

package org.apache.spark.streaming

import java.io.{File, NotSerializableException}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

import org.apache.commons.io.FileUtils
import org.scalatest.{Assertions, BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.Utils


class StreamingContextSuite extends SparkFunSuite with BeforeAndAfter with Timeouts with Logging {

  val master = "local[2]"
  val appName = this.getClass.getSimpleName
  val batchDuration = Milliseconds(500)//毫秒
  val sparkHome = "someDir"
  val envPair = "key" -> "value"
  val conf = new SparkConf().setMaster(master).setAppName(appName)

  var sc: SparkContext = null
  var ssc: StreamingContext = null

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  test("from no conf constructor") {//来自一个没有配置的构造函数
    ssc = new StreamingContext(master, appName, batchDuration)
    assert(ssc.sparkContext.conf.get("spark.master") === master)
    assert(ssc.sparkContext.conf.get("spark.app.name") === appName)
  }

  test("from no conf + spark home") {//来自一个没有Spark name的构造函数
    ssc = new StreamingContext(master, appName, batchDuration, sparkHome, Nil)
    assert(ssc.conf.get("spark.home") === sparkHome)
  }

  test("from no conf + spark home + env") {
    ssc = new StreamingContext(master, appName, batchDuration,
      sparkHome, Nil, Map(envPair))
    assert(ssc.conf.getExecutorEnv.contains(envPair))
  }

  test("from conf with settings") {//从配置设置
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.cleaner.ttl", "10s")//
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.conf.getTimeAsSeconds("spark.cleaner.ttl", "-1") === 10)
  }

  test("from existing SparkContext") {//从现有sparkcontext
    sc = new SparkContext(master, appName)
    ssc = new StreamingContext(sc, batchDuration)
  }

  test("from existing SparkContext with settings") {//从现有的sparkcontext设置
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.cleaner.ttl", "10s")
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.conf.getTimeAsSeconds("spark.cleaner.ttl", "-1") === 10)
  }

  test("from checkpoint") {//来自检查点
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    //参数的原意是清除超过这个时间的所有RDD数据,以便腾出空间给后来的RDD使用。
    //周期性清除保证在这个时间之前的元数据会被遗忘,
    //对于那些运行了几小时或者几天的Spark作业（特别是Spark Streaming）设置这个是很有用的
    myConf.set("spark.cleaner.ttl", "10s")
    val ssc1 = new StreamingContext(myConf, batchDuration)
    addInputStream(ssc1).register()
    ssc1.start()
    //
    val cp = new Checkpoint(ssc1, Time(1000))
    assert(
      Utils.timeStringAsSeconds(cp.sparkConfPairs
          .toMap.getOrElse("spark.cleaner.ttl", "-1")) === 10)
    ssc1.stop()
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))
    assert(newCp.createSparkConf().getTimeAsSeconds("spark.cleaner.ttl", "-1") === 10)
    ssc = new StreamingContext(null, newCp, null)
    assert(ssc.conf.getTimeAsSeconds("spark.cleaner.ttl", "-1") === 10)
  }

  test("checkPoint from conf") {//检查点来自一个配置文件
    //创建一个临时目录,获得绝对路径
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    //设置检查点路径
    myConf.set("spark.streaming.checkpoint.directory", checkpointDirectory)
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.checkpointDir != null)
  }

  test("state matching") {//状态匹配
    import StreamingContextState._
    assert(INITIALIZED === INITIALIZED)//初始化
    assert(INITIALIZED != ACTIVE)//活动状态
  }

  test("start and stop state check") {//启动和停止状态检查
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    //初始化
    assert(ssc.getState() === StreamingContextState.INITIALIZED)
    //活动
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)    
    ssc.stop()
    //暂停
    assert(ssc.getState() === StreamingContextState.STOPPED)

    // Make sure that the SparkContext is also stopped by default
    //确保sparkcontext默认停止
    intercept[Exception] {
      ssc.sparkContext.makeRDD(1 to 10)
    }
  }

  test("start with non-seriazable DStream checkpoints") {//开始非序列化DStream检查点
    val checkpointDir = Utils.createTempDir()
    ssc = new StreamingContext(conf, batchDuration)
    //检查点
    ssc.checkpoint(checkpointDir.getAbsolutePath)
    addInputStream(ssc).foreachRDD { rdd =>
      // Refer to this.appName from inside closure so that this closure refers to
      // the instance of StreamingContextSuite, and is therefore not serializable
      rdd.count() + appName
    }

    // Test whether start() fails early when checkpointing is enabled
    //测试是否start()失败早在检查点启用
    val exception = intercept[NotSerializableException] {
      ssc.start()
    }
    assert(exception.getMessage().contains("DStreams with their functions are not serializable"))
    assert(ssc.getState() !== StreamingContextState.ACTIVE)
    assert(StreamingContext.getActive().isEmpty)
  }

  test("start failure should stop internal components") {//启动故障应该停止内部组件
    ssc = new StreamingContext(conf, batchDuration)
    val inputStream = addInputStream(ssc)
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      Some(values.sum + state.getOrElse(0))
    }
    //没有设置检查点
    inputStream.map(x => (x, 1)).updateStateByKey[Int](updateFunc)
    // Require that the start fails because checkpoint directory was not set
    //要求启动失败，因为检查点目录没有设置
    intercept[Exception] {
      ssc.start()
    }
    assert(ssc.getState() === StreamingContextState.STOPPED)
    assert(ssc.scheduler.isStarted === false)
  }

  test("start should set job group and description of streaming jobs correctly") {
    //开始应正确设置作业组和正确的流作业的描述
    ssc = new StreamingContext(conf, batchDuration)
    ssc.sc.setJobGroup("non-streaming", "non-streaming", true)
    val sc = ssc.sc

    @volatile var jobGroupFound: String = ""
    @volatile var jobDescFound: String = ""
    @volatile var jobInterruptFound: String = ""
    @volatile var allFound: Boolean = false

    addInputStream(ssc).foreachRDD { rdd =>
      jobGroupFound = sc.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)
      jobDescFound = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      jobInterruptFound = sc.getLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL)
      allFound = true
    }
    ssc.start()

    eventually(timeout(10 seconds), interval(10 milliseconds)) {
      assert(allFound === true)
    }

    // Verify streaming jobs have expected thread-local properties
    //验证流作业有预期本地线程的属性
    assert(jobGroupFound === null)
    assert(jobDescFound.contains("Streaming job from"))
    assert(jobInterruptFound === "false")

    // Verify current thread's thread-local properties have not changed
    //验证当前线程的线程本地属性没有更改
    assert(sc.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID) === "non-streaming")
    assert(sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION) === "non-streaming")
    assert(sc.getLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL) === "true")
  }

  test("start multiple times") {//运行多个实时处理
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)
  }

  test("stop multiple times") {//暂停多个实时处理
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.start()
    ssc.stop()
    assert(ssc.getState() === StreamingContextState.STOPPED)
    ssc.stop()
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("stop before start") {//运行之前停止不抛异常
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    //停止之前启动不应该抛出异常
    ssc.stop()  // stop before start should not throw exception
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("start after stop") {//停止后启动
    // Regression test for SPARK-4301
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.stop()
    intercept[IllegalStateException] {
      //启动后停止应抛出异常
      ssc.start() // start after stop should throw exception
    }
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("stop only streaming context") {//只停止流上下文
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    // Explicitly do not stop SparkContext,显示不能暂停
    ssc = new StreamingContext(conf, batchDuration)
    sc = ssc.sparkContext
    addInputStream(ssc).register()//将当前有Dstream注册到DstreamGrap的输出流中
    ssc.start()
    ssc.stop(stopSparkContext = false)//只暂停实时流处理,不暂停Spark
    assert(ssc.getState() === StreamingContextState.STOPPED)
    assert(sc.makeRDD(1 to 100).collect().size === 100)
    sc.stop()

    // Implicitly do not stop SparkContext,隐式,不暂停Spark上下文信息
    conf.set("spark.streaming.stopSparkContextByDefault", "false")
    ssc = new StreamingContext(conf, batchDuration)
    sc = ssc.sparkContext
    addInputStream(ssc).register()//将当前有Dstream注册到DstreamGrap的输出流中
    ssc.start()
    ssc.stop()
    assert(sc.makeRDD(1 to 100).collect().size === 100)
    sc.stop()
  }

  test("stop(stopSparkContext=true) after stop(stopSparkContext=false)") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()//将当前有Dstream注册到DstreamGrap的输出流中
    ssc.stop(stopSparkContext = false)//
    assert(ssc.sc.makeRDD(1 to 100).collect().size === 100)
    ssc.stop(stopSparkContext = true)
    // Check that the SparkContext is actually stopped:
    //检查sparkcontext实际停止了
    intercept[Exception] {
      ssc.sc.makeRDD(1 to 100).collect()
    }
  }

  test("stop gracefully") {//优雅地 gracefully
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.cleaner.ttl", "3600s")
    sc = new SparkContext(conf)
    for (i <- 1 to 4) {
      logInfo("==================================\n\n\n")
      ssc = new StreamingContext(sc, Milliseconds(100))
      @volatile var runningCount = 0
      TestReceiver.counter.set(1)
      val input = ssc.receiverStream(new TestReceiver)
      input.count().foreachRDD { rdd =>
        val count = rdd.first()
        runningCount += count.toInt
        logInfo("Count = " + count + ", Running count = " + runningCount)
      }
      ssc.start()
      //最终 含有时间因素,表示目前还未发生,但终将发生
      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(runningCount > 0)
      }
      ssc.stop(stopSparkContext = false, stopGracefully = true)
      logInfo("Running count = " + runningCount)
      logInfo("TestReceiver.counter = " + TestReceiver.counter.get())
      assert(
        TestReceiver.counter.get() == runningCount + 1,
        "Received records = " + TestReceiver.counter.get() + ", " +
          "processed records = " + runningCount
      )
      Thread.sleep(100)
    }
  }
//优雅地停止即使接收器漏掉stopreceiver
  test("stop gracefully even if a receiver misses StopReceiver") {
    // This is not a deterministic unit. But if this unit test is flaky, then there is definitely
    // something wrong. See SPARK-5681
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Milliseconds(100))
    val input = ssc.receiverStream(new TestReceiver)
    input.foreachRDD(_ => {})
    ssc.start()
    // Call `ssc.stop` at once so that it's possible that the receiver will miss "StopReceiver"
    failAfter(30000 millis) {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  test("stop slow receiver gracefully") {//优雅地停止的接收器
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.streaming.gracefulStopTimeout", "20000s")
    sc = new SparkContext(conf)
    logInfo("==================================\n\n\n")
    ssc = new StreamingContext(sc, Milliseconds(100))
    var runningCount = 0
    SlowTestReceiver.receivedAllRecords = false
    // Create test receiver that sleeps in onStop()
    val totalNumRecords = 15
    val recordsPerSecond = 1
    //接收流
    val input = ssc.receiverStream(new SlowTestReceiver(totalNumRecords, recordsPerSecond))
    input.count().foreachRDD { rdd =>
      val count = rdd.first()
      runningCount += count.toInt
      logInfo("Count = " + count + ", Running count = " + runningCount)
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(500)//
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    logInfo("Running count = " + runningCount)
    assert(runningCount > 0)
    assert(runningCount == totalNumRecords)
    Thread.sleep(100)
  }
  //注册和注销的streamingSource
  test ("registering and de-registering of streamingSource") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    ssc = new StreamingContext(conf, batchDuration)
    assert(ssc.getState() === StreamingContextState.INITIALIZED)
    addInputStream(ssc).register()
    ssc.start()

    val sources = StreamingContextSuite.getSources(ssc.env.metricsSystem)
    val streamingSource = StreamingContextSuite.getStreamingSource(ssc)
    assert(sources.contains(streamingSource))
    assert(ssc.getState() === StreamingContextState.ACTIVE)

    ssc.stop()
    val sourcesAfterStop = StreamingContextSuite.getSources(ssc.env.metricsSystem)
    val streamingSourceAfterStop = StreamingContextSuite.getStreamingSource(ssc)
    assert(ssc.getState() === StreamingContextState.STOPPED)
    assert(!sourcesAfterStop.contains(streamingSourceAfterStop))
  }

  test("awaitTermination") {//用于等待子线程结束
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    // test whether start() blocks indefinitely or not
    //测试是否无限期运行块或不
    failAfter(2000 millis) {
      ssc.start()
    }

    // test whether awaitTermination() exits after give amount of time
    //测试是否awaittermination()退出后给时间
    failAfter(1000 millis) {
      ssc.awaitTerminationOrTimeout(500)
    }

    // test whether awaitTermination() does not exit if not time is given
    //测试是否awaittermination()不存在退出,如果不是时间给定
    val exception = intercept[Exception] {
      failAfter(1000 millis) {
        ssc.awaitTermination()
        throw new Exception("Did not wait for stop")
      }
    }
    assert(exception.isInstanceOf[TestFailedDueToTimeoutException], "Did not wait for stop")

    var t: Thread = null
    // test whether wait exits if context is stopped
    //测试是否等待退出,如果上下文停止
    failAfter(10000 millis) { // 10 seconds because spark takes a long time to shutdown
      t = new Thread() {
        override def run() {
          Thread.sleep(500)
          ssc.stop()
        }
      }
      t.start()
      ssc.awaitTermination()
    }
    // SparkContext.stop will set SparkEnv.env to null. We need to make sure SparkContext is stopped
    // before running the next test. Otherwise, it's possible that we set SparkEnv.env to null after
    // the next test creates the new SparkContext and fail the test.
    t.join()
  }

  test("awaitTermination after stop") {//等待终止后停止
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    failAfter(10000 millis) {
      ssc.start()
      ssc.stop()
      ssc.awaitTermination()
    }
  }

  test("awaitTermination with error in task") {//等待任务终止错误
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream
      .map { x => throw new TestException("error in map task"); x }
      .foreachRDD(_.count())

    val exception = intercept[Exception] {
      ssc.start()
      ssc.awaitTerminationOrTimeout(5000)
    }
    assert(exception.getMessage.contains("map task"), "Expected exception not thrown")
  }
//awaitTermination 有工作生成的错误
  test("awaitTermination with error in job generation") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.transform { rdd => throw new TestException("error in transform"); rdd }.register()
    val exception = intercept[TestException] {
      ssc.start()
      ssc.awaitTerminationOrTimeout(5000)
    }
    assert(exception.getMessage.contains("transform"), "Expected exception not thrown")
  }
//awaitTermination超时
  test("awaitTerminationOrTimeout") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    ssc.start()

    // test whether awaitTerminationOrTimeout() return false after give amount of time
    //测试是否awaitterminationortimeout()返回false给之后的时间
    failAfter(1000 millis) {
      assert(ssc.awaitTerminationOrTimeout(500) === false)
    }

    var t: Thread = null
    // test whether awaitTerminationOrTimeout() return true if context is stopped
    //10秒,因为Spark需要很长的时间关机
    failAfter(10000 millis) { // 10 seconds because spark takes a long time to shutdown
      t = new Thread() {
        override def run() {
          Thread.sleep(500)
          ssc.stop()
        }
      }
      t.start()
      assert(ssc.awaitTerminationOrTimeout(10000) === true)
    }
    // SparkContext.stop will set SparkEnv.env to null. We need to make sure SparkContext is stopped
    // before running the next test. Otherwise, it's possible that we set SparkEnv.env to null after
    // the next test creates the new SparkContext and fail the test.
    t.join()
  }

  test("getOrCreate") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    // Function to create StreamingContext that has a config to identify it to be new context
    //函数创建StreamingContext,确定它是新的配置上下文
    var newContextCreated = false
    def creatingFunction(): StreamingContext = {
      newContextCreated = true
      new StreamingContext(conf, batchDuration)
    }

    // Call ssc.stop after a body of code
    def testGetOrCreate(body: => Unit): Unit = {
      newContextCreated = false
      try {
        body
      } finally {
        if (ssc != null) {
          ssc.stop()
        }
        ssc = null
      }
    }

    val emptyPath = Utils.createTempDir().getAbsolutePath()

    // getOrCreate should create new context with empty path
    //应该用空的路径创建新的上下文
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(emptyPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    val corruptedCheckpointPath = createCorruptedCheckpoint()

    // getOrCreate should throw exception with fake checkpoint file and createOnError = false
    //应该抛出假检查点文件的异常
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(corruptedCheckpointPath, creatingFunction _)
    }

    // getOrCreate should throw exception with fake checkpoint file
    //应该抛出假检查点文件的异常
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = false)
    }

    // getOrCreate should create new context with fake checkpoint file and createOnError = true
    //应该创建新的上下文与假检查点文件
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = true)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    val checkpointPath = createValidCheckpoint()

    // getOrCreate should recover context with checkpoint path, and recover old configuration
    //应恢复检查点路径的上下文,并恢复旧的配置
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(checkpointPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(ssc.conf.get("someKey") === "someValue", "checkpointed config not recovered")
    }

    // getOrCreate should recover StreamingContext with existing SparkContext
    //要恢复现有的StreamingContext with existing SparkContext
    testGetOrCreate {
      sc = new SparkContext(conf)
      ssc = StreamingContext.getOrCreate(checkpointPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(!ssc.conf.contains("someKey"), "checkpointed config unexpectedly recovered")
    }
  }

  test("getActive and getActiveOrCreate") {//获得活动和创建
    require(StreamingContext.getActive().isEmpty, "context exists from before")
    sc = new SparkContext(conf)

    var newContextCreated = false

    def creatingFunc(): StreamingContext = {
      newContextCreated = true
      val newSsc = new StreamingContext(sc, batchDuration)
      val input = addInputStream(newSsc)
      input.foreachRDD { rdd => rdd.count }
      newSsc
    }

    def testGetActiveOrCreate(body: => Unit): Unit = {
      newContextCreated = false
      try {
        body
      } finally {

        if (ssc != null) {
          ssc.stop(stopSparkContext = false)
        }
        ssc = null
      }
    }

    // getActiveOrCreate should create new context and getActive should return it only
    // after starting the context
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(creatingFunc _)
      assert(ssc != null, "no context created")
      assert(newContextCreated === true, "new context not created")
      assert(StreamingContext.getActive().isEmpty,
        "new initialized context returned before starting")
      ssc.start()
      assert(StreamingContext.getActive() === Some(ssc),
        "active context not returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) === ssc,
        "active context not returned")
      ssc.stop()
      assert(StreamingContext.getActive().isEmpty,
        "inactive context returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) !== ssc,
        "inactive context returned")
    }

    // getActiveOrCreate and getActive should return independently created context after activating
    testGetActiveOrCreate {
      ssc = creatingFunc()  // Create
      assert(StreamingContext.getActive().isEmpty,
        "new initialized context returned before starting")
      ssc.start()
      assert(StreamingContext.getActive() === Some(ssc),
        "active context not returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) === ssc,
        "active context not returned")
      ssc.stop()
      assert(StreamingContext.getActive().isEmpty,
        "inactive context returned")
    }
  }

  test("getActiveOrCreate with checkpoint") {
    // Function to create StreamingContext that has a config to identify it to be new context
    //函数创建StreamingContext,配置以确定它是新的上下文
    var newContextCreated = false
    def creatingFunction(): StreamingContext = {
      newContextCreated = true
      new StreamingContext(conf, batchDuration)
    }

    // Call ssc.stop after a body of code
    def testGetActiveOrCreate(body: => Unit): Unit = {
      //没有活动的背景
      require(StreamingContext.getActive().isEmpty) // no active context
      newContextCreated = false
      try {
        body
      } finally {
        if (ssc != null) {
          ssc.stop()
        }
        ssc = null
      }
    }

    val emptyPath = Utils.createTempDir().getAbsolutePath()
    val corruptedCheckpointPath = createCorruptedCheckpoint()
    val checkpointPath = createValidCheckpoint()

    // getActiveOrCreate should return the current active context if there is one
    //如果有一个,应该返回当前的活动上下文
    testGetActiveOrCreate {
      ssc = new StreamingContext(
        conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock"), batchDuration)
      addInputStream(ssc).register()
      ssc.start()
      val returnedSsc = StreamingContext.getActiveOrCreate(checkpointPath, creatingFunction _)
      assert(!newContextCreated, "new context created instead of returning")
      assert(returnedSsc.eq(ssc), "returned context is not the activated context")
    }

    // getActiveOrCreate should create new context with empty path
    //应该用空的路径创建新的上下文
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(emptyPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    // getActiveOrCreate should throw exception with fake checkpoint file and createOnError = false
    //getActiveOrCreate 应该抛出假检查点文件的异常并createOnError = false
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(corruptedCheckpointPath, creatingFunction _)
    }

    // getActiveOrCreate should throw exception with fake checkpoint file
    //getActiveOrCreate 应该抛出假检查点文件的异常
    intercept[Exception] {
      ssc = StreamingContext.getActiveOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = false)
    }

    // getActiveOrCreate should create new context with fake
    // checkpoint file and createOnError = true
    //应该用假检查点文件和createonerror =true的创造新的上下文
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = true)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    // getActiveOrCreate should recover context with checkpoint path, and recover old configuration
    //getActiveOrCreate 应恢复检查点路径的上下文,恢复旧配置
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(checkpointPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(ssc.conf.get("someKey") === "someValue")
    }
  }

  test("multiple streaming contexts") {//多个流上下文
    sc = new SparkContext(
      conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock"))
    ssc = new StreamingContext(sc, Seconds(1))
    val input = addInputStream(ssc)
    input.foreachRDD { rdd => rdd.count }
    ssc.start()

    // Creating another streaming context should not create errors
    //创建另一个流上下文不应该创建错误
    val anotherSsc = new StreamingContext(sc, Seconds(10))
    val anotherInput = addInputStream(anotherSsc)
    anotherInput.foreachRDD { rdd => rdd.count }

    val exception = intercept[IllegalStateException] {
      anotherSsc.start()
    }
    assert(exception.getMessage.contains("StreamingContext"), "Did not get the right exception")
  }
  //dstream产生RDD创建网站
  test("DStream and generated RDD creation sites") {
    testPackage.test()
  }

  test("throw exception on using active or stopped context") {//使用主动或停止上下文抛出异常
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
    ssc = new StreamingContext(conf, batchDuration)
    require(ssc.getState() === StreamingContextState.INITIALIZED)
    val input = addInputStream(ssc)
    val transformed = input.map { x => x}
    transformed.foreachRDD { rdd => rdd.count }

    def testForException(clue: String, expectedErrorMsg: String)(body: => Unit): Unit = {
      withClue(clue) {
        val ex = intercept[IllegalStateException] {
          body
        }
        assert(ex.getMessage.toLowerCase().contains(expectedErrorMsg))
      }
    }

    ssc.start()
    require(ssc.getState() === StreamingContextState.ACTIVE)
    //启动后添加输入没有错误
    testForException("no error on adding input after start", "start") {
      addInputStream(ssc) }
    //启动后添加转换没有错误
    testForException("no error on adding transformation after start", "start") {
      input.map { x => x * 2 } }
    //启动后添加输出操作没有错误
    testForException("no error on adding output operation after start", "start") {
      transformed.foreachRDD { rdd => rdd.collect() } }

    ssc.stop()
    require(ssc.getState() === StreamingContextState.STOPPED)
    //停止后添加输入时没有错误
    testForException("no error on adding input after stop", "stop") {
      addInputStream(ssc) }
    //停止后添加转换没有错误
    testForException("no error on adding transformation after stop", "stop") {
      input.map { x => x * 2 } }
    //停止后添加输出操作没有错误
    testForException("no error on adding output operation after stop", "stop") {
      transformed.foreachRDD { rdd => rdd.collect() } }
  }
  //流不支持检查点队列
  test("queueStream doesn't support checkpointing") {
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    def creatingFunction(): StreamingContext = {
      val _ssc = new StreamingContext(conf, batchDuration)
      val rdd = _ssc.sparkContext.parallelize(1 to 10)
      _ssc.checkpoint(checkpointDirectory)
      _ssc.queueStream[Int](Queue(rdd)).register()
      _ssc
    }
    ssc = StreamingContext.getOrCreate(checkpointDirectory, creatingFunction _)
    ssc.start()
    eventually(timeout(10000 millis)) {
      assert(Checkpoint.getCheckpointFiles(checkpointDirectory).size > 1)
    }
    ssc.stop()
    val e = intercept[SparkException] {
      ssc = StreamingContext.getOrCreate(checkpointDirectory, creatingFunction _)
    }
    // StreamingContext.validate changes the message, so use "contains" here
    assert(e.getCause.getMessage.contains("queueStream doesn't support checkpointing. " +
        //请不要用queuestream当检查点启用
      "Please don't use queueStream when checkpointing is enabled."))
  }
  //创建一个InputDStream,但不使用它不应该崩溃
  test("Creating an InputDStream but not using it should not crash") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val input1 = addInputStream(ssc)
    val input2 = addInputStream(ssc)
    val output = new TestOutputStream(input2)
    output.register()
    val batchCount = new BatchCounter(ssc)
    ssc.start()
    // Just wait for completing 2 batches to make sure it triggers
    //只等待完成2个批次，以确保它触发
    // `DStream.getMaxInputStreamRememberDuration`
    batchCount.waitUntilBatchesCompleted(2, 10000)
    // Throw the exception if crash
    //如果崩溃抛出异常
    ssc.awaitTerminationOrTimeout(1)
    ssc.stop()
  }

  def addInputStream(s: StreamingContext): DStream[Int] = {
    val input = (1 to 100).map(i => 1 to i)
    val inputStream = new TestInputStream(s, input, 1)
    inputStream
  }

  def createValidCheckpoint(): String = {
    val testDirectory = Utils.createTempDir().getAbsolutePath()
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    ssc = new StreamingContext(conf.clone.set("someKey", "someValue"), batchDuration)
    ssc.checkpoint(checkpointDirectory)
    ssc.textFileStream(testDirectory).foreachRDD { rdd => rdd.count() }
    ssc.start()
    eventually(timeout(10000 millis)) {
      assert(Checkpoint.getCheckpointFiles(checkpointDirectory).size > 1)
    }
    ssc.stop()
    checkpointDirectory
  }

  def createCorruptedCheckpoint(): String = {
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    val fakeCheckpointFile = Checkpoint.checkpointFile(checkpointDirectory, Time(1000))
    FileUtils.write(new File(fakeCheckpointFile.toString()), "blablabla")
    assert(Checkpoint.getCheckpointFiles(checkpointDirectory).nonEmpty)
    checkpointDirectory
  }
}

class TestException(msg: String) extends Exception(msg)

/** 
 *  Custom receiver for testing whether all data received by a receiver gets processed or not
 *  用于测试接收由接收器的接书所有数据是否被处理或不被处理的自定义接收机器
 *  */
class TestReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) with Logging {

  var receivingThreadOption: Option[Thread] = None

  def onStart() {
    val thread = new Thread() {
      override def run() {
        logInfo("Receiving started")
        while (!isStopped) {
          store(TestReceiver.counter.getAndIncrement)
        }
        logInfo("Receiving stopped at count value of " + TestReceiver.counter.get())
      }
    }
    receivingThreadOption = Some(thread)
    thread.start()
  }

  def onStop() {
    // no clean to be done, the receiving thread should stop on it own, so just wait for it.
    //没有干净的完成,接收线程应该停止在它自己的,所以只是等待它
    receivingThreadOption.foreach(_.join())
  }
}

object TestReceiver {
  val counter = new AtomicInteger(1)
}

/** 
 *  Custom receiver for testing whether a slow receiver can be shutdown gracefully or not
 *  用于测试一个缓慢的接收器是否可以优雅地关闭的自定义接收器 
 *  */
class SlowTestReceiver(totalRecords: Int, recordsPerSecond: Int)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) with Logging {

  var receivingThreadOption: Option[Thread] = None

  def onStart() {
    val thread = new Thread() {
      override def run() {
        logInfo("Receiving started")
        for(i <- 1 to totalRecords) {
          Thread.sleep(1000 / recordsPerSecond)
          store(i)
        }
        SlowTestReceiver.receivedAllRecords = true
        logInfo(s"Received all $totalRecords records")
      }
    }
    receivingThreadOption = Some(thread)
    thread.start()
  }

  def onStop() {
    // Simulate slow receiver by waiting for all records to be produced
    //通过等待所有的记录来模拟缓慢的接收器
    while (!SlowTestReceiver.receivedAllRecords) {
      Thread.sleep(100)
    }
    // no clean to be done, the receiving thread should stop on it own
  }
}

object SlowTestReceiver {
  var receivedAllRecords = false
}

/** 
 *  Streaming application for testing DStream and RDD creation sites
 *  流的测试dstream和RDD创建网站中的应用 
 *  */
package object testPackage extends Assertions {
  def test() {
    val conf = new SparkConf().setMaster("local").setAppName("CreationSite test")
    val ssc = new StreamingContext(conf , Milliseconds(100))
    try {
      val inputStream = ssc.receiverStream(new TestReceiver)

      // Verify creation site of DStream
      //验证创建dstream
      val creationSite = inputStream.creationSite
      assert(creationSite.shortForm.contains("receiverStream") &&
        creationSite.shortForm.contains("StreamingContextSuite")
      )
      assert(creationSite.longForm.contains("testPackage"))

      // Verify creation site of generated RDDs
      //验证生成的RDDS创作现场
      var rddGenerated = false
      var rddCreationSiteCorrect = false
      var foreachCallSiteCorrect = false

      inputStream.foreachRDD { rdd =>
        rddCreationSiteCorrect = rdd.creationSite == creationSite
        foreachCallSiteCorrect =
          rdd.sparkContext.getCallSite().shortForm.contains("StreamingContextSuite")
        rddGenerated = true
      }
      ssc.start()

      eventually(timeout(10000 millis), interval(10 millis)) {
        assert(rddGenerated && rddCreationSiteCorrect, "RDD creation site was not correct")
        assert(rddGenerated && foreachCallSiteCorrect, "Call site in foreachRDD was not correct")
      }
    } finally {
      ssc.stop()
    }
  }
}

/**
 * Helper methods for testing StreamingContextSuite
 * 测试streamingcontextsuite辅助方法
 * This includes methods to access private methods and fields in StreamingContext and MetricsSystem
 * 这包括方法访问私有方法和在StreamingContext和metricssystem领域
 */
private object StreamingContextSuite extends PrivateMethodTester {
  private val _sources = PrivateMethod[ArrayBuffer[Source]]('sources)
  private def getSources(metricsSystem: MetricsSystem): ArrayBuffer[Source] = {
    metricsSystem.invokePrivate(_sources())
  }
  private val _streamingSource = PrivateMethod[StreamingSource]('streamingSource)
  private def getStreamingSource(streamingContext: StreamingContext): StreamingSource = {
    streamingContext.invokePrivate(_streamingSource())
  }
}
