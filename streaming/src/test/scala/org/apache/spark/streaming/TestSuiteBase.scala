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

import java.io.{ObjectInputStream, IOException}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.SynchronizedBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.scalatest.BeforeAndAfter
import org.scalatest.time.{Span, Seconds => ScalaTestSeconds}
import org.scalatest.concurrent.Eventually.timeout
import org.scalatest.concurrent.PatienceConfiguration

import org.apache.spark.{Logging, SparkConf, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ForEachDStream}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{ManualClock, Utils}

/**
 * A dummy stream that does absolutely nothing.
 * 一个没有任何的虚拟流
 */
private[streaming] class DummyDStream(ssc: StreamingContext) extends DStream[Int](ssc) {
  override def dependencies: List[DStream[Int]] = List.empty
  override def slideDuration: Duration = Seconds(1) //1秒
  override def compute(time: Time): Option[RDD[Int]] = Some(ssc.sc.emptyRDD[Int])
}

/**
 * A dummy input stream that does absolutely  nothing.
 * 一个没有任何的虚拟输入流
 */
private[streaming] class DummyInputDStream(ssc: StreamingContext) extends InputDStream[Int](ssc) {
  override def start(): Unit = { }
  override def stop(): Unit = { }
  override def compute(time: Time): Option[RDD[Int]] = Some(ssc.sc.emptyRDD[Int])
}

/**
 * This is a input stream just for the testsuites. This is equivalent(相当的) to a checkpointable,
 * 这只是一个输入流的测试包,这相当于一个检查点,像Kafka可重复、可靠的消息队列,它需要一个序列作为输入
 * replayable(可重复), reliable(可靠) message queue like Kafka. It requires a sequence as input, and
 * 返回在i_th批下手动时钟的i_th
 * returns the i_th element at the i_th batch unde manual clock.
 */
class TestInputStream[T: ClassTag](ssc_ : StreamingContext, input: Seq[Seq[T]], numPartitions: Int)
  extends InputDStream[T](ssc_) {

  def start() {}

  def stop() {}

  def compute(validTime: Time): Option[RDD[T]] = {
    //计算RDD时间
    logInfo("Computing RDD for time " + validTime)
    //索引开始,validTime 有效的时间,slide Duration持续时间
    val index = ((validTime - zeroTime) / slideDuration - 1).toInt
    //
    val selectedInput = if (index < input.size) input(index) else Seq[T]()

    // lets us test cases where RDDs are not created
    // RDDs没有创建测试用例
    if (selectedInput == null) {
      return None
    }

    // Report the input data's information to InputInfoTracker for testing
    //代表测试数据输入的信息
    val inputInfo = StreamInputInfo(id, selectedInput.length.toLong)
    //Inputinfotracker里面是保存了元数据
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    val rdd = ssc.sc.makeRDD(selectedInput, numPartitions)
    logInfo("Created RDD " + rdd.id + " with " + selectedInput)
    Some(rdd)
  }
}

/**
 * This is a output stream just for the testsuites. All the output is collected into a
 * ArrayBuffer. This buffer is wiped clean on being restored from checkpoint.
 * 这只是一个输出流的测试包,所有输出都被集合到ArrayBuffer,这个缓冲区是清除干净的从检查点恢复
 * The buffer contains a sequence of RDD's, each containing a sequence of items
 * 缓冲区包含一个序列的RDD,每个包含一系列的项目
 */
class TestOutputStream[T: ClassTag](
    parent: DStream[T],
    val output: SynchronizedBuffer[Seq[T]] =
      new ArrayBuffer[Seq[T]] with SynchronizedBuffer[Seq[T]]
  ) extends ForEachDStream[T](parent, (rdd: RDD[T], t: Time) => {
    val collected = rdd.collect()
    output += collected
  }) {

  // This is to clear the output buffer every it is read from a checkpoint
  // 这是为了清除输出缓冲区,每一个它是从一个检查点读取
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    output.clear()
  }
}

/**
 * This is a output stream just for the testsuites. All the output is collected into a
 * 这只是一个输出流的测试包,所有的输出被收集成一个ArrayBuffer
 * ArrayBuffer. This buffer is wiped clean on being restored from checkpoint.
 * 这个缓冲区是清除干净的从检查点恢复,缓冲区包含一个序列的RDD,每个包含一个分区的序列
 * The buffer contains a sequence of RDD's, each containing a sequence of partitions, each
 * containing a sequence of items.
 * 每个包含一个项目序列
 */
class TestOutputStreamWithPartitions[T: ClassTag](
    parent: DStream[T],
    val output: SynchronizedBuffer[Seq[Seq[T]]] =
      new ArrayBuffer[Seq[Seq[T]]] with SynchronizedBuffer[Seq[Seq[T]]])
  extends ForEachDStream[T](parent, (rdd: RDD[T], t: Time) => {
    val collected = rdd.glom().collect().map(_.toSeq)
    output += collected
  }) {

  // This is to clear the output buffer every it is read from a checkpoint
  //这是为了清除输出缓冲区,每一个它是从一个检查点读取
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    output.clear()
  }
}

/**
 * An object that counts the number of started / completed batches. This is implemented using a
 * StreamingListener. Constructing a new instance automatically registers a StreamingListener on
 * the given StreamingContext.
 * 一个开始/完成批次数量计数的对象,这是使用一个流侦听器来实现的,
 * 构建一个新的实例会自动在给定的流上下文中注册一个流侦听器。
 */
class BatchCounter(ssc: StreamingContext) {

  // All access to this state should be guarded by `BatchCounter.this.synchronized`
  //所有访问状态都应该由`BatchCounter.this.synchronized`保护
  private var numCompletedBatches = 0
  private var numStartedBatches = 0

  private val listener = new StreamingListener {
    override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit =
      BatchCounter.this.synchronized {
        numStartedBatches += 1
        BatchCounter.this.notifyAll()
      }
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =
      BatchCounter.this.synchronized {
        numCompletedBatches += 1
        BatchCounter.this.notifyAll()
      }
  }
  ssc.addStreamingListener(listener)

  def getNumCompletedBatches: Int = this.synchronized {
    numCompletedBatches
  }

  def getNumStartedBatches: Int = this.synchronized {
    numStartedBatches
  }

  /**
   * Wait until `expectedNumCompletedBatches` batches are completed, or timeout. Return true if
   * `expectedNumCompletedBatches` batches are completed. Otherwise, return false to indicate it's
   * timeout.
   * 等到` expectednumcompletedbatches `批次完成,如果expectedNumCompletedBatches批次完成返回true
   * 否则返回false,指示它的超时
   *
   * @param expectedNumCompletedBatches the `expectedNumCompletedBatches` batches to wait
   * @param timeout the maximum time to wait in milliseconds.
   */
  def waitUntilBatchesCompleted(expectedNumCompletedBatches: Int, timeout: Long): Boolean =
    waitUntilConditionBecomeTrue(numCompletedBatches >= expectedNumCompletedBatches, timeout)

  /**
   * Wait until `expectedNumStartedBatches` batches are completed, or timeout. Return true if
   * `expectedNumStartedBatches` batches are completed. Otherwise, return false to indicate it's
   * timeout.
   * 等到` expectednumcompletedbatches `批次完成,如果expectedNumCompletedBatches批次完成返回true
   * 否则返回false,指示它的超时
   * 
   * @param expectedNumStartedBatches the `expectedNumStartedBatches` batches to wait
   * @param timeout the maximum time to wait in milliseconds.
   */
  def waitUntilBatchesStarted(expectedNumStartedBatches: Int, timeout: Long): Boolean =
    waitUntilConditionBecomeTrue(numStartedBatches >= expectedNumStartedBatches, timeout)

  private def waitUntilConditionBecomeTrue(condition: => Boolean, timeout: Long): Boolean = {
    synchronized {
      var now = System.currentTimeMillis()
      val timeoutTick = now + timeout
      while (!condition && timeoutTick > now) {
        wait(timeoutTick - now)
        now = System.currentTimeMillis()
      }
      condition
    }
  }
}

/**
 * This is the base trait for Spark Streaming testsuites. This provides basic functionality
 * to run user-defined set of input on user-defined stream operations, and verify the output.
 * 这是一个基本Spark Streaming测试套件,这提供输入流自定义的操作的基本功能并验证输出
 */
trait TestSuiteBase extends SparkFunSuite with BeforeAndAfter with Logging {

  // Name of the framework for Spark context
  //Spark上下文的框架名称
  def framework: String = this.getClass.getSimpleName

  // Master for Spark context  
  //Spark Master上下文
  def master: String = "local[2]"

  // Batch duration
  // 间隔时间1秒
  def batchDuration: Duration = Seconds(1)

  // Directory where the checkpoint data will be saved
  //检查点数据被保存目录
  lazy val checkpointDir: String = {
    val dir = Utils.createTempDir()
    logDebug(s"checkpointDir: $dir")
    dir.toString
  }

  // Number of partitions of the input parallel collections created for testing
  //用于测试的输入并行集合的分区数
  def numInputPartitions: Int = 2

  // Maximum time to wait before the test times out
  //超时前等待的最大时间,即超时1000毫秒,
  def maxWaitTimeMillis: Int = 10000

  // Whether to use manual clock or not
  //是否使用手动时钟
  def useManualClock: Boolean = true

  // Whether to actually wait in real time before changing manual clock
  //等待之前是否实时在改变人工时钟
  def actuallyWait: Boolean = false

  // A SparkConf to use in tests. Can be modified before calling setupStreams to configure things.
  //一个sparkconf使用测试,可以修改调用setupstreams配置之前
  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(framework)

  // Timeout for use in ScalaTest `eventually` blocks
  //超时使用 
  val eventuallyTimeout: PatienceConfiguration.Timeout = timeout(Span(10, ScalaTestSeconds))

  // Default before function for any streaming test suite. Override this
  // if you want to add your stuff to "before" (i.e., don't call before { } )
  //默认任何流测试套件的功能之前,重写此,如果你想添加你的东西
  def beforeFunction() {
    if (useManualClock) {
      logInfo("Using manual clock")
      //手动时间
      conf.set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
    } else {
      logInfo("Using real clock")
      //系统时间
      conf.set("spark.streaming.clock", "org.apache.spark.util.SystemClock")
    }
  }

  // Default after function for any streaming test suite. Override this
  // if you want to add your stuff to "after" (i.e., don't call after { } )
  //默认功能任何流测试套件,如果你想添加你的东西到“after”重写
  def afterFunction() {
    System.clearProperty("spark.streaming.clock")
  }

  before(beforeFunction)
  after(afterFunction)

  /**
   * Run a block of code with the given StreamingContext and automatically
   * 给定的StreamingContext自动运行一个代码块
   * stop the context when the block completes or when an exception is thrown.
   * 停止上下文完成块或抛出异常
   */
  def withStreamingContext[R](ssc: StreamingContext)(block: StreamingContext => R): R = {
    try {
      block(ssc)
    } finally {
      try {
        ssc.stop(stopSparkContext = true)
      } catch {
        case e: Exception =>
          logError("Error stopping StreamingContext", e)
      }
    }
  }

  /**
   * Run a block of code with the given TestServer and automatically
   * stop the server when the block completes or when an exception is thrown.
   * 给定的TestServer自动运行一个代码块,停止server完成块或抛出异常
   * 柯里化(Currying)指的是将原来接受两个参数的函数变成新的接受一个参数的函数的过程。
   * 新的函数返回一个以原有第二个参数为参数的函数
   */
  def withTestServer[R](testServer: TestServer)(block: TestServer => R): R = {
    try {
      block(testServer)
    } finally {
      try {
        testServer.stop()
      } catch {
        case e: Exception =>
          logError("Error stopping TestServer", e)
      }
    }
  }

  /**
   * Set up required DStreams to test the DStream operation using the two sequences
   * of input collections.
   * 设置所需dstreams测试dstream,操作使用两个序列的输入集合
   */
  def setupStreams[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      numPartitions: Int = numInputPartitions
    ): StreamingContext = {
    // Create StreamingContext,创建实时上下文
    val ssc = new StreamingContext(conf, batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir)//设置检查点
    }

    // Setup the stream computation
    //设置流计算
    val inputStream = new TestInputStream(ssc, input, numPartitions)
    //匿名operation函数接收DStream[U]类型,返回DStream[V]
    //
    val operatedStream = operation(inputStream)
    val outputStream = new TestOutputStreamWithPartitions(operatedStream,
      new ArrayBuffer[Seq[Seq[V]]] with SynchronizedBuffer[Seq[Seq[V]]])
    outputStream.register()
    ssc
  }

  /**
   * Set up required DStreams to test the binary operation using the sequence
   * of input collections.
   * 设置所需的dstreams测试使用两个序列的输入集合操作
   */
  def setupStreams[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W]
    ): StreamingContext = {
    // Create StreamingContext
    val ssc = new StreamingContext(conf, batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir)
    }

    // Setup the stream computation
    //设置流计算
    val inputStream1 = new TestInputStream(ssc, input1, numInputPartitions)
    val inputStream2 = new TestInputStream(ssc, input2, numInputPartitions)
    val operatedStream = operation(inputStream1, inputStream2)
    val outputStream = new TestOutputStreamWithPartitions(operatedStream,
      new ArrayBuffer[Seq[Seq[W]]] with SynchronizedBuffer[Seq[Seq[W]]])
    outputStream.register()
    ssc
  }

  /**
   * Runs the streams set up in `ssc` on manual clock for `numBatches` batches and
   * returns the collected output. It will wait until `numExpectedOutput` number of
   * output data has been collected or timeout (set by `maxWaitTimeMillis`) is reached.
   * 运行设置` SSC `手动时钟` numbatches `数,返回集合输出,  它会等到 `numExpectedOutput`数输出数据完成或超时
   * Returns a sequence of items for each RDD.
   * 返回一个序列的每个RDD项目
   */
  def runStreams[V: ClassTag](
      ssc: StreamingContext,
      numBatches: Int,
      numExpectedOutput: Int
    ): Seq[Seq[V]] = {
    // Flatten each RDD into a single Seq
    //把每个RDD生成单个的序列
    runStreamsWithPartitions(ssc, numBatches, numExpectedOutput).map(_.flatten.toSeq)
  }

  /**
   * Runs the streams set up in `ssc` on manual clock for `numBatches` batches and
   * 运行流设置StreamingContext,手动时钟的`numBatches`批次,返回集合输出,
   *  returns the collected output. It will wait until `numExpectedOutput` number of
   * output data has been collected or timeout (set by `maxWaitTimeMillis`) is reached.
   *
   * Returns a sequence of RDD's. Each RDD is represented as several sequences of items, each
   * representing one partition.
   * 返回一个序列的RDD,每RDD代表的项目数序列,每一个代表一个分区
   */
  def runStreamsWithPartitions[V: ClassTag](
      ssc: StreamingContext,
      numBatches: Int,
      numExpectedOutput: Int
    ): Seq[Seq[Seq[V]]] = {
    assert(numBatches > 0, "Number of batches to run stream computation is zero")
    assert(numExpectedOutput > 0, "Number of expected outputs after " + numBatches + " is zero")
    logInfo("numBatches = " + numBatches + ", numExpectedOutput = " + numExpectedOutput)

    // Get the output buffer
    //获取输出缓冲区
    val outputStream = ssc.graph.getOutputStreams.
      filter(_.isInstanceOf[TestOutputStreamWithPartitions[_]]).
      head.asInstanceOf[TestOutputStreamWithPartitions[V]]
    val output = outputStream.output

    try {
      // Start computation
      //开始计算
      ssc.start()

      // Advance manual clock 提前设置时钟
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      logInfo("Manual clock before advancing = " + clock.getTimeMillis())
      //实际等待
      if (actuallyWait) {//默认值false
        for (i <- 1 to numBatches) {
          logInfo("Actually waiting for " + batchDuration)
          clock.advance(batchDuration.milliseconds)
          Thread.sleep(batchDuration.milliseconds)
        }
      } else {
        logInfo("Actually waiting for " + numBatches * batchDuration.milliseconds)
        clock.advance(numBatches * batchDuration.milliseconds)
      }
      logInfo("Manual clock after advancing = " + clock.getTimeMillis())

      // Wait until expected number of output items have been generated
      //等待直到生成的输出期望值
      val startTime = System.currentTimeMillis()
      while (output.size < numExpectedOutput &&
        System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
        logInfo("output.size = " + output.size + ", numExpectedOutput = " + numExpectedOutput)
        ssc.awaitTerminationOrTimeout(50)
      }
      //当前时间-开始运行时间
      val timeTaken = System.currentTimeMillis() - startTime
      logInfo("Output generated in " + timeTaken + " milliseconds")
      output.foreach(x => logInfo("[" + x.mkString(",") + "]"))
      //操作超时后,maxWaitTimeMillis超时等待时间
      assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
      assert(output.size === numExpectedOutput, "Unexpected number of outputs generated")
      //给旧RDDS完成一些时间
      Thread.sleep(100) // Give some time for the forgetting old RDDs to complete
    } finally {
      ssc.stop(stopSparkContext = true)
    }
    output
  }

  /**
   * Verify whether the output values after running a DStream operation
   * 验证是否输出值运行一个dstream操作
   * is same as the expected output values, by comparing the output
   * 和预期的输出值相同,通过比较输出集合,无论是列表(命令的事项)或集(顺序不重要)
   * collections either as lists (order matters) or sets (order does not matter)
   */
  def verifyOutput[V: ClassTag](
      output: Seq[Seq[V]],
      expectedOutput: Seq[Seq[V]],
      useSet: Boolean
    ) {
    logInfo("--------------------------------")
    logInfo("output.size = " + output.size)
    logInfo("output")
    //output序列输出值
    output.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    //期望值
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Match the output with the expected output
    //将输出与预期输出相匹配
    for (i <- 0 until output.size) {
      if (useSet) {//使用Set集合,默认false
       //logInfo(output(i).toSet.to+"==="+expectedOutput(i).toSet)
        assert(
          output(i).toSet === expectedOutput(i).toSet,
          s"Set comparison failed\n" +
            s"Expected output (${expectedOutput.size} items):\n${expectedOutput.mkString("\n")}\n" +
            s"Generated output (${output.size} items): ${output.mkString("\n")}"
        )
      } else {
         logInfo(output(i).toList+"==="+ expectedOutput(i).toList)
        assert(
          output(i).toList === expectedOutput(i).toList,
          s"Ordered list comparison failed\n" +
            s"Expected output (${expectedOutput.size} items):\n${expectedOutput.mkString("\n")}\n" +
            s"Generated output (${output.size} items): ${output.mkString("\n")}"
        )
      }
    }
    logInfo("Output verified successfully")
  }

  /**
   * Test unary DStream operation with a list of inputs, with number of
   * 试验一元dstream操作与列表的输入,批量运行的数量与预期的输出值相同
   * batches to run same as the number of expected output values
   */
  def testOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],//期望输出
      useSet: Boolean = false
    ) {
    testOperation[U, V](input, operation, expectedOutput, -1, useSet)
  }

  /**
   * Test unary(一元) DStream operation with a list of inputs
   * 试验一元dstream操作与列表输入
   * [U: ClassTag, V: ClassTag]泛型方法 
   * @param input      Sequence of input collections 输入集合序列
   * @param operation  Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections 期望输出集合的序列
   * @param numBatches Number of batches to run the operation for 批量运行的操作数
   * @param useSet     Compare the output values with the expected output values
   *                   as sets (order matters) or as lists (order does not matter)
   *                   将输出值与预期的输出值(顺序事项)或列表(排序不重要)进行比较
   */
  def testOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      numBatches: Int,
      useSet: Boolean
    ) {
    //批量操作
    //默认numBatches的值-1
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    withStreamingContext(
        //设置运行的参数
        setupStreams[U, V](input, operation)
        ){ ssc =>//匿名
      //运行
      val output = runStreams[V](ssc, numBatches_, expectedOutput.size)
      verifyOutput[V](output, expectedOutput, useSet)
    }
  }

  /**
   * Test binary DStream operation with two lists of inputs, with number of
   * batches to run same as the number of expected output values
   * 试验二dstream操作两列输入,随着批量运行的数量与预期的输出值相同
   */
  def testOperation[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W],
      expectedOutput: Seq[Seq[W]],
      useSet: Boolean
    ) {
    testOperation[U, V, W](input1, input2, operation, expectedOutput, -1, useSet)
  }

  /**
   * Test binary DStream operation with two lists of inputs
   * 试验二个dstream操作两列输入
   * @param input1     First sequence of input collections 第一个输入序列的集合
   * @param input2     Second sequence of input collections 第二个输入序列的集合
   * @param operation  Binary DStream operation to be applied to the 2 inputs
   * 									 二个dstream操作被应用到2个输入
   * @param expectedOutput Sequence of expected output collections
   * 											 期望输出集合的序列
   * @param numBatches Number of batches to run the operation for
   * 									 批量运行的操作数
   * @param useSet     Compare the output values with the expected output values
   *                   as sets (order matters) or as lists (order does not matter)
   *                   	将输出值与预期的输出值进行比较为集(顺序事项)或列表(顺序不重要)
   */
  def testOperation[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W],
      expectedOutput: Seq[Seq[W]],
      numBatches: Int,
      useSet: Boolean
    ) {
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    withStreamingContext(setupStreams[U, V, W](input1, input2, operation)) { ssc =>
      val output = runStreams[W](ssc, numBatches_, expectedOutput.size)
      verifyOutput[W](output, expectedOutput, useSet)
    }
  }
}
