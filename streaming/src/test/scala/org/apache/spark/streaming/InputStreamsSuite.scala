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

import java.io.{File, BufferedWriter, OutputStreamWriter}
import java.net.{Socket, SocketException, ServerSocket}
import java.nio.charset.Charset
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit, ArrayBlockingQueue}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer, SynchronizedQueue}
import scala.language.postfixOps

import com.google.common.io.Files
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.util.{ManualClock, Utils}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.Receiver
/**
 * 输入流测试套件
 */
class InputStreamsSuite extends TestSuiteBase with BeforeAndAfter {

  test("socket input stream") {//网络输入流
    withTestServer(new TestServer()) { testServer =>
      // Start the server
      //开始运行服务器
      
      testServer.start()
     
      // Set up the streaming context and input streams
      //设置流上下文和输入流
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        ssc.addStreamingListener(ssc.progressListener)

        val input = Seq(1, 2, 3, 4, 5)
        // Use "batchCount" to make sure we check the result after all batches finish
        //使用“batchcount”检查所有批次完成后的结果,注册一个监听器
        val batchCounter = new BatchCounter(ssc)//
        //连接测试服务器
        val networkStream = ssc.socketTextStream(
          "localhost", testServer.port, StorageLevel.MEMORY_AND_DISK)
        //同步ArrayBuffer,一个时间内只能有一个线程得到执行
        val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
        val outputStream = new TestOutputStream(networkStream, outputBuffer)
        //register将当前DStream注册到DStreamGraph的输出流中
        outputStream.register()
        ssc.start()

        // Feed data to the server to send to the network receiver
        //将数据发送到服务器发送到网络接收器
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        val expectedOutput = input.map(_.toString)
        for (i <- 0 until input.size) {
          testServer.send(input(i).toString + "\n")
          Thread.sleep(500)//500毫秒,0.5秒
          //批量间隔为毫秒
          clock.advance(batchDuration.milliseconds)
        }
        // Make sure we finish all batches before "stop"
        //等待直到批处理完成,3000毫秒超时
        if (!batchCounter.waitUntilBatchesCompleted(input.size, 30000)) {
          fail("Timeout: cannot finish all batches in 30 seconds")
        }

        // Ensure progress listener has been notified of all events
        //确保进度侦听器已通知所有事件
        ssc.scheduler.listenerBus.waitUntilEmpty(500)

        // Verify all "InputInfo"s have been reported
        //检查所有inputinfo的报告
        assert(ssc.progressListener.numTotalReceivedRecords === input.size)
        assert(ssc.progressListener.numTotalProcessedRecords === input.size)

        logInfo("Stopping server")
        testServer.stop()
        logInfo("Stopping context")
        ssc.stop()

        // Verify whether data received was as expected
        //验证是否收到的数据是预期的
        logInfo("--------------------------------")
        logInfo("output.size = " + outputBuffer.size)
        logInfo("output")
        outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
        logInfo("expected output.size = " + expectedOutput.size)
        logInfo("expected output")
        expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
        logInfo("--------------------------------")

        // Verify whether all the elements received are as expected
        //验证是否所有的元素都将收到
        // (whether the elements were received one in each interval is not verified)
        //是否收到一个在每个时间间隔中的元素没有被验证
        val output: ArrayBuffer[String] = outputBuffer.flatMap(x => x)
        assert(output.size === expectedOutput.size)
        for (i <- 0 until output.size) {
          assert(output(i) === expectedOutput(i))
        }
      }
    }
  }

  test("socket input stream - no block in a batch") {//批理处理没有块
    withTestServer(new TestServer()) { testServer =>
      testServer.start()

      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        ssc.addStreamingListener(ssc.progressListener)

        val batchCounter = new BatchCounter(ssc)
        val networkStream = ssc.socketTextStream(
          "localhost", testServer.port, StorageLevel.MEMORY_AND_DISK)
        val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
        val outputStream = new TestOutputStream(networkStream, outputBuffer)
        outputStream.register()
        ssc.start()

        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        clock.advance(batchDuration.milliseconds)

        // Make sure the first batch is finished
        //确保第一批完成
        if (!batchCounter.waitUntilBatchesCompleted(1, 30000)) {//没有块处理
          fail("Timeout: cannot finish all batches in 30 seconds")
        }

        networkStream.generatedRDDs.foreach { case (_, rdd) =>
          assert(!rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
        }
      }
    }
  }

  test("binary records stream") {//二进制数据流
    val testDir: File = null
    try {
      val batchDuration = Seconds(2)//2秒
      val testDir = Utils.createTempDir()//创建临时目录
      // Create a file that exists before the StreamingContext is created:
      //创建一个文件存在之前,创建StreamingContext
      val existingFile = new File(testDir, "0")
      Files.write("0\n", existingFile, Charset.forName("UTF-8"))
      assert(existingFile.setLastModified(10000) && existingFile.lastModified === 10000)

      // Set up the streaming context and input streams
      //设置流上下文和输入流
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        // This `setTime` call ensures that the clock is past the creation time of `existingFile`
        //这"setTime"调用确保时钟过去的创作时间"existingfile"
        clock.setTime(existingFile.lastModified + batchDuration.milliseconds)
        val batchCounter = new BatchCounter(ssc)
        //二进制记录流,长度为1
        val fileStream = ssc.binaryRecordsStream(testDir.toString, 1)
        val outputBuffer = new ArrayBuffer[Seq[Array[Byte]]]
          with SynchronizedBuffer[Seq[Array[Byte]]]
        val outputStream = new TestOutputStream(fileStream, outputBuffer)
        outputStream.register()
        ssc.start()

        // Advance the clock so that the files are created after StreamingContext starts, 
        // but not enough to trigger a batch,但不足以触发一个批处理
        clock.advance(batchDuration.milliseconds / 2)

        val input = Seq(1, 2, 3, 4, 5)
        input.foreach { i =>
          Thread.sleep(batchDuration.milliseconds)
          val file = new File(testDir, i.toString)
          Files.write(Array[Byte](i.toByte), file)
          assert(file.setLastModified(clock.getTimeMillis()))
          assert(file.lastModified === clock.getTimeMillis())
          logInfo("Created file " + file)
          // Advance the clock after creating the file to avoid a race when
          //推进时钟后创建该文件,以避免竞争时
          // setting its modification time
          //设置修改时间
          clock.advance(batchDuration.milliseconds)
          eventually(eventuallyTimeout) {
            assert(batchCounter.getNumCompletedBatches === i)
          }
        }
        //期望输出
        val expectedOutput = input.map(i => i.toByte)
        //实际输出
        val obtainedOutput = outputBuffer.flatten.toList.map(i => i(0).toByte)
        assert(obtainedOutput === expectedOutput)
      }
    } finally {
      if (testDir != null) Utils.deleteRecursively(testDir)
    }
  }

  test("file input stream - newFilesOnly = true") {//文件输入流
    testFileStream(newFilesOnly = true)
  }

  test("file input stream - newFilesOnly = false") {
    testFileStream(newFilesOnly = false)
  }

  test("multi-thread receiver") {//多线程接收
    // set up the test receiver
    //多线程接收数据
    val numThreads = 10
    //记录线程数
    val numRecordsPerThread = 1000
    //
    val numTotalRecords = numThreads * numRecordsPerThread
    //测试多线程接收数据
    val testReceiver = new MultiThreadTestReceiver(numThreads, numRecordsPerThread)
    MultiThreadTestReceiver.haveAllThreadsFinished = false

    // set up the network stream using the test receiver
    //使用测试接收器设置网络流
    val ssc = new StreamingContext(conf, batchDuration)
    //数据源输入
    val networkStream = ssc.receiverStream[Int](testReceiver)
    val countStream = networkStream.count
    
    val outputBuffer = new ArrayBuffer[Seq[Long]] with SynchronizedBuffer[Seq[Long]]
    
    val outputStream = new TestOutputStream(countStream, outputBuffer)
    def output: ArrayBuffer[Long] = outputBuffer.flatMap(x => x)
    outputStream.register()
    ssc.start()

    // Let the data from the receiver be received
    //来自接收者的数据
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val startTime = System.currentTimeMillis()
    while((!MultiThreadTestReceiver.haveAllThreadsFinished || output.sum < numTotalRecords) &&
      System.currentTimeMillis() - startTime < 5000) {
      Thread.sleep(100)
      clock.advance(batchDuration.milliseconds)
    }
    Thread.sleep(1000)
    logInfo("Stopping context")
    ssc.stop()

    // Verify whether data received was as expected
    //验证数据是否收到预期
    logInfo("--------------------------------")
    logInfo("output.size = " + outputBuffer.size)
    logInfo("output")
    outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")
    assert(output.sum === numTotalRecords)
  }

  test("queue input stream - oneAtATime = true") {//队列输入流,一次一个
    // Set up the streaming context and input streams
    //设置流上下文和输入流
    val ssc = new StreamingContext(conf, batchDuration)
    val queue = new SynchronizedQueue[RDD[String]]()//
    val queueStream = ssc.queueStream(queue, oneAtATime = true)//一次一个
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]//
    val outputStream = new TestOutputStream(queueStream, outputBuffer)
    def output: ArrayBuffer[Seq[String]] = outputBuffer.filter(_.size > 0)
    outputStream.register()
    ssc.start()

    // Setup data queued into the stream
    //设置队列到流中的数据
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq("1", "2", "3", "4", "5")
    val expectedOutput = input.map(Seq(_))

    val inputIterator = input.toIterator
    for (i <- 0 until input.size) {
      // Enqueue more than 1 item per tick but they should dequeue one at a time
      //将超过1项每滴答应出列一次
      inputIterator.take(2).foreach(i => queue += ssc.sparkContext.makeRDD(Seq(i)))
      clock.advance(batchDuration.milliseconds)
    }
    Thread.sleep(1000)
    logInfo("Stopping context")
    ssc.stop()

    // Verify whether data received was as expected
    //验证数据是否收到预期
    logInfo("--------------------------------")
    logInfo("output.size = " + outputBuffer.size)
    logInfo("output")
    outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Verify whether all the elements received are as expected
    //检查所有接收到的元素是否预期
    assert(output.size === expectedOutput.size)
    for (i <- 0 until output.size) {
      assert(output(i) === expectedOutput(i))
    }
  }

  test("queue input stream - oneAtATime = false") {//排队输入流
    // Set up the streaming context and input streams
    //设置流上下文和输入流
    val ssc = new StreamingContext(conf, batchDuration)
    val queue = new SynchronizedQueue[RDD[String]]()
    val queueStream = ssc.queueStream(queue, oneAtATime = false)
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
    val outputStream = new TestOutputStream(queueStream, outputBuffer)
    def output: ArrayBuffer[Seq[String]] = outputBuffer.filter(_.size > 0)
    outputStream.register()
    ssc.start()

    // Setup data queued into the stream
    //设置队列到流中的数据
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq("1", "2", "3", "4", "5")
    val expectedOutput = Seq(Seq("1", "2", "3"), Seq("4", "5"))

    // Enqueue the first 3 items (one by one), they should be merged in the next batch
    //第一入列 3项(一对一)的,他们应该出现在下一批
    val inputIterator = input.toIterator
    inputIterator.take(3).foreach(i => queue += ssc.sparkContext.makeRDD(Seq(i)))
    clock.advance(batchDuration.milliseconds)
    Thread.sleep(1000)

    // Enqueue the remaining items (again one by one), merged in the final batch
    inputIterator.foreach(i => queue += ssc.sparkContext.makeRDD(Seq(i)))
    clock.advance(batchDuration.milliseconds)
    Thread.sleep(1000)
    logInfo("Stopping context")
    ssc.stop()

    // Verify whether data received was as expected
    //验证收到的数据是否预期
    logInfo("--------------------------------")
    logInfo("output.size = " + outputBuffer.size)
    logInfo("output")
    outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Verify whether all the elements received are as expected
    //检查所有接收到的元素是否预期
    assert(output.size === expectedOutput.size)
    for (i <- 0 until output.size) {
      assert(output(i) === expectedOutput(i))
    }
  }

  test("test track the number of input stream") {//跟踪输入流为数字
    withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>

      class TestInputDStream extends InputDStream[String](ssc) {
        def start() {}

        def stop() {}
	//compute 在指定时间生成一个RDD
        def compute(validTime: Time): Option[RDD[String]] = None
      }

      class TestReceiverInputDStream extends ReceiverInputDStream[String](ssc) {
        def getReceiver: Receiver[String] = null
      }

      // Register input streams
      //注册输入流
      val receiverInputStreams = Array(new TestReceiverInputDStream, new TestReceiverInputDStream)
      val inputStreams = Array(new TestInputDStream, new TestInputDStream, new TestInputDStream)

      assert(ssc.graph.getInputStreams().length ==
        receiverInputStreams.length + inputStreams.length)
      assert(ssc.graph.getReceiverInputStreams().length == receiverInputStreams.length)
      assert(ssc.graph.getReceiverInputStreams() === receiverInputStreams)
      //tabulate 返回包含一个给定的函数的值超过从0开始的范围内的整数值的数组
      assert(ssc.graph.getInputStreams().map(_.id) === Array.tabulate(5)(i => i))
      assert(receiverInputStreams.map(_.id) === Array(0, 1))
    }
  }

  def testFileStream(newFilesOnly: Boolean) {
    val testDir: File = null
    try {
      //批处理时间
      val batchDuration = Seconds(2)//秒
      //创建临时文件
      val testDir = Utils.createTempDir()
      // Create a file that exists before the StreamingContext is created:
      //创建一个文件存在之前,创建StreamingContext
      val existingFile = new File(testDir, "0")
      //输出存在文件
      Files.write("0\n", existingFile, Charset.forName("UTF-8"))
      //setLastModified 新的最后修改时间
      assert(existingFile.setLastModified(10000) && existingFile.lastModified === 10000)

      // Set up the streaming context and input streams
      //设置流上下文和输入流
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        // This `setTime` call ensures that the clock is past the creation time of `existingFile`
        //设置
        clock.setTime(existingFile.lastModified + batchDuration.milliseconds)
        val batchCounter = new BatchCounter(ssc)
        val fileStream = ssc.fileStream[LongWritable, Text, TextInputFormat](
          testDir.toString, (x: Path) => true, newFilesOnly = newFilesOnly).map(_._2.toString)
        val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
        val outputStream = new TestOutputStream(fileStream, outputBuffer)
        outputStream.register()
        ssc.start()

        // Advance the clock so that the files are created after StreamingContext starts, but
        // not enough to trigger a batch
        clock.advance(batchDuration.milliseconds / 2)

        // Over time, create files in the directory
        //随着时间的推移,在目录中创建文件
        val input = Seq(1, 2, 3, 4, 5)
        input.foreach { i =>
          val file = new File(testDir, i.toString)
          Files.write(i + "\n", file, Charset.forName("UTF-8"))
          assert(file.setLastModified(clock.getTimeMillis()))
          assert(file.lastModified === clock.getTimeMillis())
          logInfo("Created file " + file)
          // Advance the clock after creating the file to avoid a race when
          // setting its modification time 设置修改时间
          clock.advance(batchDuration.milliseconds)
          eventually(eventuallyTimeout) {
            assert(batchCounter.getNumCompletedBatches === i)
          }
        }

        // Verify that all the files have been read
        //验证所有的文件已被读取
        val expectedOutput = if (newFilesOnly) {
          input.map(_.toString).toSet
        } else {
          (Seq(0) ++ input).map(_.toString).toSet
        }
        assert(outputBuffer.flatten.toSet === expectedOutput)
      }
    } finally {
      if (testDir != null) Utils.deleteRecursively(testDir)
    }
  }
}


/** 
 *  This is a server to test the network input stream
 *  这是一个测试网络输入流的服务器 
 *  */
class TestServer(portToBind: Int = 0) extends Logging {
 //ArrayBlockingQueue是一个由数组支持的有界阻塞队列,此队列按 FIFO(先进先出)原则对元素进行排序。
 //让容量满时往BlockingQueue中添加数据时会造成阻塞,当容量为空时取元素操作会阻塞
  val queue = new ArrayBlockingQueue[String](100)
  //服务器端需要创建监听端口的 ServerSocket,ServerSocket负责接收客户连接请求
  //如果把参数 portToBind设为 0,表示由操作系统来为服务器分配一个任意可用的端口
  val serverSocket = new ServerSocket(portToBind)
  //CountDownLatch,在完成一组正在其他线程中执行的操作之前,它允许一个或多个线程一直等待.
  //是一个同步计数器,构造时传入int参数,该参数就是计数器的初始值,每调用一次countDown()方法,
  //计数器减1,计数器大于0 时,await()方法会阻塞程序继续执行
  private val startLatch = new CountDownLatch(1)

  val servingThread = new Thread() {
    override def run() {
      try {
        while (true) {
          logInfo("Accepting connections on port " + port)
          val clientSocket = serverSocket.accept()//等待接收客户连接请求
          if (startLatch.getCount == 1) {
            // The first connection is a test connection to implement "waitForStart", so skip it
            // and send a signal
            //第一连接是连接是测试连接“waitforstart”,所以跳过它发送一个信号
            if (!clientSocket.isClosed) {
              clientSocket.close()//关闭客户端连接
            }
           //每调用一次countDown()方法,计数器减1,计数器大于0 时,await()方法会阻塞程序继续执行 
            startLatch.countDown()
          } else {
            // Real connections
            // 实际的连接
            logInfo("New connection")
            try {
              //客户端向服务器发送数据时,会根据数据包的大小决定是否立即发送
              clientSocket.setTcpNoDelay(true)
              //创建一个使用默认大小输出缓冲区的缓冲字符输出流
              val outputStream = new BufferedWriter(
               //返回客户端输出流,把字节流转化为字符流
                new OutputStreamWriter(clientSocket.getOutputStream))

              while (clientSocket.isConnected) {
                //相当于先get然后再remove掉,参数指定等待的毫秒数,无论I/O是否准备好,poll都会返回
                val msg = queue.poll(100, TimeUnit.MILLISECONDS)
                if (msg != null) {
                  //将文本写入字符输出流,缓冲各个字符
                  outputStream.write(msg)
                  //刷新该流的缓冲
                  outputStream.flush()
                  logInfo("Message '" + msg + "' sent")
                }
              }
            } catch {
              case e: SocketException => logError("TestServer error", e)
            } finally {
              logInfo("Connection closed")
              if (!clientSocket.isClosed) {
                clientSocket.close()
              }
            }
          }
        }
      } catch {
        case ie: InterruptedException =>

      } finally {
        serverSocket.close()
      }
    }
  }

  def start(): Unit = {
    servingThread.start()
    //如果服务器启动,如果指定时间启动则返回false
    if (!waitForStart(10000)) {
      stop()
      throw new AssertionError("Timeout: TestServer cannot start in 10 seconds")
    }
  }

  /**
   * Wait until the server starts. Return true if the server starts in "millis" milliseconds.
   * Otherwise, return false to indicate it's timeout.
   * 等待服务器启动,如果服务器在"millis"毫秒内启动返回true,否则,返回false,表示它启动超时
   */
  private def waitForStart(millis: Long): Boolean = {
    // We will create a test connection to the server so that we can make sure it has started.
    //将创建一个测试连接到服务器,以便我们可以确保它已经启动
    val socket = new Socket("localhost", port)
    try {
      //使当前线程在锁存器倒计数至零之前一直等待，除非线程被中断或超出了指定的等待时间     
      startLatch.await(millis, TimeUnit.MILLISECONDS)//timeout -(毫秒)要等待的最长时间
    } finally {
      if (!socket.isClosed) {
        socket.close()
      }
    }
  }
  //发送消息
  def send(msg: String) { queue.put(msg) }
  //在等待时被中断
  def stop() { servingThread.interrupt() }
  //获得本地端口
  def port: Int = serverSocket.getLocalPort
}

/** 
 *  This is a receiver to test multiple threads inserting data using block generator 
 *  这是一个测试多线程生成插入数据块接收器
 *  */
class MultiThreadTestReceiver(numThreads: Int, numRecordsPerThread: Int)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY_SER) with Logging {
  lazy val executorPool = Executors.newFixedThreadPool(numThreads)//线程池数
  lazy val finishCount = new AtomicInteger(0)//AtomicInteger则通过一种线程安全的加减操作接口

  def onStart() {
    (1 to numThreads).map(threadId => {
      val runnable = new Runnable {
        def run() {
          (1 to numRecordsPerThread).foreach(i =>
            //存储接收到的数据到Spark的内存中的单个项目
            store(threadId * numRecordsPerThread + i) )
          if (finishCount.incrementAndGet == numThreads) {//
            MultiThreadTestReceiver.haveAllThreadsFinished = true
          }
          logInfo("Finished thread " + threadId)
        }
      }
      //提交线程池
      executorPool.submit(runnable)
    })
  }

  def onStop() {
    executorPool.shutdown()
  }
}

object MultiThreadTestReceiver {
  var haveAllThreadsFinished = false
}
