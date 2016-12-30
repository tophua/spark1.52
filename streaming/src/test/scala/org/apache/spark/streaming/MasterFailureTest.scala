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

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.util.UUID

import com.google.common.io.Files

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

/**
 * Master(主节点故障)测试套件
 */
private[streaming]
object MasterFailureTest extends Logging {

  @volatile var killed = false
  @volatile var killCount = 0
  @volatile var setupCalled = false

  def main(args: Array[String]) {
    // scalastyle:off println
    //批量大小以毫秒为单位
 /*   if (args.size < 2) {
      println(
        "Usage: MasterFailureTest <local/HDFS directory> <# batches> " +
          "[<batch size in milliseconds>]")
      System.exit(1)
    }*/
    //检查点
   // val directory = args(0)
     val directory = "D:\\checkpoint_test"
    // val directory="hdfs://xcsq:8089/analytics/"
    //批量时间
   // val numBatches = args(1).toInt//批量
      val numBatches =10.toInt//批量
    //设置合理的批处理时间,一般500ms性能很不错了。Milliseconds 毫秒
    val batchDuration = if (args.size > 2) Milliseconds(args(2).toInt) else Seconds(1)//1秒

    println("\n\n========================= MAP TEST =========================\n\n")
    testMap(directory, numBatches, batchDuration)

    println("\n\n================= UPDATE-STATE-BY-KEY TEST =================\n\n")
    testUpdateStateByKey(directory, numBatches, batchDuration)

    println("\n\nSUCCESS\n\n")
    // scalastyle:on println
  }
  //
  def testMap(directory: String, numBatches: Int, batchDuration: Duration) {
    // Input: time=1 ==> [ 1 ] , time=2 ==> [ 2 ] , time=3 ==> [ 3 ] , ...
    //input: scala.collection.immutable.Seq[String] = Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val input = (1 to numBatches).map(_.toString).toSeq
    // Expected output: time=1 ==> [ 1 ] , time=2 ==> [ 2 ] , time=3 ==> [ 3 ] , ...
    //expectedOutput= Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val expectedOutput = (1 to numBatches)

    val operation = (st: DStream[String]) => st.map(_.toInt)

    // Run streaming operation with multiple master failures
    //具有多个主故障的运行流操作
    val output = testOperation(directory, batchDuration, input, operation, expectedOutput)

    logInfo("Expected output, size = " + expectedOutput.size)
    logInfo(expectedOutput.mkString("[", ",", "]"))
    logInfo("Output, size = " + output.size)
    logInfo(output.mkString("[", ",", "]"))

    // Verify whether all the values of the expected output is present
    // in the output
    //是否验证所期望输出的所有值是否存在于输出中
    assert(output.distinct.toSet == expectedOutput.toSet)
  }


  def testUpdateStateByKey(directory: String, numBatches: Int, batchDuration: Duration) {
    // Input: time=1 ==> [ a ] , time=2 ==> [ a, a ] , time=3 ==> [ a, a, a ] , ...
    val input = (1 to numBatches).map(i => (1 to i).map(_ => "a").mkString(" ")).toSeq
    // Expected output: time=1 ==> [ (a, 1) ] , time=2 ==> [ (a, 3) ] , time=3 ==> [ (a,6) ] , ...
    val expectedOutput = (1L to numBatches).map(i => (1L to i).sum).map(j => ("a", j))

    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Long], state: Option[Long]) => {
        Some(values.foldLeft(0L)(_ + _) + state.getOrElse(0L))
      }
 /**
  *updateStateByKey 操作返回一个新状态的DStream,
  *其中传入的函数基于键之前的状态和键新的值更新每个键的状态
  *updateStateByKey操作对每个键会调用一次,
  *values表示键对应的值序列,state可以是任务状态
  **/
      st.flatMap(_.split(" "))
        .map(x => (x, 1L))
        .updateStateByKey[Long](updateFunc)
	      //在interval周期后给生成的RDD设置检查点
        .checkpoint(batchDuration * 5)
    }

    // Run streaming operation with multiple master failures
    //具有多个主故障的运行流操作
    val output = testOperation(directory, batchDuration, input, operation, expectedOutput)

    logInfo("Expected output, size = " + expectedOutput.size + "\n" + expectedOutput)
    logInfo("Output, size = " + output.size + "\n" + output)

    // Verify whether all the values in the output are among the expected output values
    //是否验证输出中的所有值是否在预期的输出值中
    output.foreach(o =>
      assert(expectedOutput.contains(o), "Expected value " + o + " not found")
    )

    // Verify whether the last expected output value has been generated, there by
    // confirming that none of the inputs have been missed
    //验证是否已生成最后一个期望输出值,确认没有输入已被丢失
    assert(output.last == expectedOutput.last)
  }

  /**
   * Tests stream operation with multiple master failures, and verifies whether the
   * final set of output values is as expected or not.
   * 测试流操作多个主节点故障,并最后验证一组输出值是否预期一致
   */
  def testOperation[T: ClassTag](
    directory: String,
    batchDuration: Duration,
    input: Seq[String],
    operation: DStream[String] => DStream[T],
    expectedOutput: Seq[T]
  ): Seq[T] = {

    // Just making sure that the expected output does not have duplicates
    //只要确保预期的输出没有重复
    assert(expectedOutput.distinct.toSet == expectedOutput.toSet)

    // Reset all state
    //重置所有状态
    reset()

    // Create the directories for this test
    //为这个测试创建随机目录
    val uuid = UUID.randomUUID().toString
    val rootDir = new Path(directory, uuid)
    val fs = rootDir.getFileSystem(new Configuration())
    //HDFS checkpoint子目录
    val checkpointDir = new Path(rootDir, "checkpoint")
    //HDFS test子目录
    val testDir = new Path(rootDir, "test")
    //创建checkpoint子目录
    fs.mkdirs(checkpointDir)
    //创建test子目录
    fs.mkdirs(testDir)

    // Setup the stream computation with the given operation
    //用给定的操作设置流计算,从检查点文件获取StreamingContext
    //创建新的StreamingContext对象,或者从检查点构造一个
    /**
    如果checkpointDirectory目录存在,则context对象会从检查点数据重新构建出来,
    如果该目录不存在(如:首次运行),则functionToCreateContext函数会被调,
    创建一个新的StreamingContext对象并定义好DStream数据流**/
    val ssc = StreamingContext.getOrCreate(checkpointDir.toString, () => {
      //返回StreamingContext
      setupStreams(batchDuration, operation, checkpointDir, testDir)
    })

    // Check if setupStream was called to create StreamingContext   
    // (and not created from checkpoint file)
    // 检查setupstream调用创建StreamingContext,并不是从检查点文件创建
    assert(setupCalled, "Setup was not called in the first call to StreamingContext.getOrCreate")

    // Start generating files in the a different thread
    // 开始生成文件在一个不同的线程
    val fileGeneratingThread = new FileGeneratingThread(input, testDir, batchDuration.milliseconds)
    //启动线程生成文件
    fileGeneratingThread.start()

    // Run the streams and repeatedly kill it until the last expected output
    // has been generated, or until it has run for twice the expected time
    //运行流，并多次杀死它，直到最后一个预期的输出已产生,或者直到它运行了两倍的预期时间
    val lastExpectedOutput = expectedOutput.last  //取出最后一个值10
    val maxTimeToRun = expectedOutput.size * batchDuration.milliseconds * 2//10x1000X2=20000
    //合并输出值
    val mergedOutput = runStreams(ssc, lastExpectedOutput, maxTimeToRun)
    //join 这个方法一直在等待,直到调用他的线程终止
    fileGeneratingThread.join()
    //hdfs 删除检查点目录
    fs.delete(checkpointDir, true)
    //hdfs 删除测试目录
    fs.delete(testDir, true)
    logInfo("Finished test after " + killCount + " failures")
    mergedOutput
  }

  /**
   * Sets up the stream computation with the given operation, directory (local or HDFS),
   * and batch duration. Returns the streaming context and the directory to which
   * files should be written for testing.
   * 用给定的操作设置流计算,目录(本地或HDFS)和批处理间隔时间,返回测试的流上下文和目录写入文件   
   */
  private def setupStreams[T: ClassTag](
      batchDuration: Duration,//时间间隔
      operation: DStream[String] => DStream[T],//流式操作
      checkpointDir: Path,//检查点目录
      testDir: Path//测试目录
    ): StreamingContext = {
    // Mark that setup was called
    // 标记setupStreams被调用
    setupCalled = true

    // Setup the streaming computation with the given operation
    //设置给定的流式计算操作,分隔的时间叫作批次间隔
    val ssc = new StreamingContext("local[4]", "MasterFailureTest", batchDuration, null, Nil,
      Map())
    //设置检查点目录
    ssc.checkpoint(checkpointDir.toString)
    //Spark Streaming将监视该dataDirectory目录,并处理该目录下任何新建的文件,不支持嵌套目录
    //注意:dataDirectory中的文件必须通过moving或者renaming来创建
    //各个文件数据格式必须一致
    //获得测试目录数据
    val inputStream = ssc.textFileStream(testDir.toString)
    //操作输入流
    val operatedStream = operation(inputStream)
    //创建一个输出对象流
    val outputStream = new TestOutputStream(operatedStream)
     //register将当前DStream注册到DStreamGraph的输出流中
    outputStream.register()
    ssc
  }


  /**
   * Repeatedly starts and kills the streaming context until timed out or
   * the last expected output is generated. Finally, return
   * 重复启动和杀死流上下文,直到超时或生成最后的期望输出,最后返回
   */
  private def runStreams[T: ClassTag](
      ssc_ : StreamingContext,
      lastExpectedOutput: T,
      maxTimeToRun: Long
   ): Seq[T] = {

    var ssc = ssc_
    //跑的总时间
    var totalTimeRan = 0L
    //是否最后一个输出生成
    var isLastOutputGenerated = false
    //是否超时
    var isTimedOut = false
    //合并输出
    val mergedOutput = new ArrayBuffer[T]()
    //检查点目录
    val checkpointDir = ssc.checkpointDir
    //批处理间隔
    val batchDuration = ssc.graph.batchDuration

    while(!isLastOutputGenerated && !isTimedOut) {
      // Get the output buffer
      //获取输出缓冲区,获取输出流TestOutputStream对象的output变量
      val outputBuffer = ssc.graph.getOutputStreams().head.asInstanceOf[TestOutputStream[T]].output
      def output = outputBuffer.flatMap(x => {
        //println("outputBuffer=="+x)
        x})

      // Start the thread to kill the streaming after some time
      // 运行线程一段时间后杀死流
      killed = false
      //线程在一个随机的时间后杀死流上下文
      val killingThread = new KillingThread(ssc, batchDuration.milliseconds * 10)
      killingThread.start()
      //运行的时间
      var timeRan = 0L
      try {
        // Start the streaming computation and let it run while ...
        //开始流式计算,并让它运行
        // (i) StreamingContext has not been shut down yet, StreamingContext没有被关闭
        // (ii) The last expected output has not been generated yet 最后的预期输出未生成
        // (iii) Its not timed out yet 它还没有超时
        System.clearProperty("spark.streaming.clock")
        System.clearProperty("spark.driver.port")
        ssc.start()
        //开始运行的时间       
        val startTime = System.currentTimeMillis()
        while (!killed && !isLastOutputGenerated && !isTimedOut) {
          Thread.sleep(100)
          timeRan = System.currentTimeMillis() - startTime
          //判断输出不为空,最后一个值output.last 10==lastExpectedOutput 10
          isLastOutputGenerated = (output.nonEmpty && output.last == lastExpectedOutput)
         //判断是否超时
          isTimedOut = (timeRan + totalTimeRan > maxTimeToRun)
        }
      } catch {
        case e: Exception => logError("Error running streaming context", e)
      }
      //isAlive如果调用他的线程仍在运行,返回true,否则返回false
      if (killingThread.isAlive) {
        //interrupt中断并不能直接终止另一个线程,而需要被中断的线程自己处理中断
        killingThread.interrupt()
        // SparkContext.stop will set SparkEnv.env to null. We need to make sure SparkContext is
        // stopped before running the next test. Otherwise, it's possible that we set SparkEnv.env
        // to null after the next test creates the new SparkContext and fail the test.
        //join 这个方法一直在等待,直到调用他的线程终止
        killingThread.join()
      }
      ssc.stop()
      /**
       * Has been killed = true
       * Is last output generated = false
       * Is timed out = false
       */
      logInfo("Has been killed = " + killed)
      logInfo("Is last output generated = " + isLastOutputGenerated)
      logInfo("Is timed out = " + isTimedOut)
      //=============================
      println("Has been killed = " + killed)
      println("Is last output generated = " + isLastOutputGenerated)
      println("Is timed out = " + isTimedOut)
    
      // Verify whether the output of each batch has only one element or no element
      //检查每个批处理的输出是否只有一个元素或没有元素
      // and then merge the new output with all the earlier output
      //然后合并新的输出与所有之前的输出
      mergedOutput ++= output
      totalTimeRan += timeRan
      /**
       * New output = ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8)
       * Merged output = ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8)
       * Time ran = 13788
       * Total time ran = 13788
       */
      logInfo("New output = " + output)
      logInfo("Merged output = " + mergedOutput)
      logInfo("Time ran = " + timeRan)
      logInfo("Total time ran = " + totalTimeRan)
      //=============================
      println("New output = " + output)
      println("Merged output = " + mergedOutput)
      println("Time ran = " + timeRan)
      println("Total time ran = " + totalTimeRan)
      if (!isLastOutputGenerated && !isTimedOut) {
        val sleepTime = Random.nextInt(batchDuration.milliseconds.toInt * 10)
        logInfo(
          "\n-------------------------------------------\n" +
            "   Restarting stream computation in " + sleepTime + " ms   " +
            "\n-------------------------------------------\n"
        )
        Thread.sleep(sleepTime)
        // Recreate the streaming context from checkpoint
        //从检查点重新创建流上下文
        ssc = StreamingContext.getOrCreate(checkpointDir, () => {
          throw new Exception("Trying to create new context when it " +
            "should be reading from checkpoint file")
        })
      }
    }
    mergedOutput
  }

  /**
   * Verifies the output value are the same as expected. Since failures can lead to
   * 验证输出值与预期相同,由于故障可以导致一个被处理的一个批次两次,一批输出可能会出现不止一次
   * a batch being processed twice, a batches output may appear more than once
   * consecutively. To avoid getting confused with those, we eliminate consecutive
   * 为了避免与那些混淆,我们消除“输出”的值的连续重复批输出,因此
   * duplicate batch outputs of values from the `output`. As a result, the
   * expected output should not have consecutive batches with the same values as output.
   * 预期的输出不应该具有与输出相同的值的连续批。
   */
  private def verifyOutput[T: ClassTag](output: Seq[T], expectedOutput: Seq[T]) {
    // Verify whether expected outputs do not consecutive batches with same output
    //验证是否预期输出不连续批具有相同的输出
    for (i <- 0 until expectedOutput.size - 1) {
      assert(expectedOutput(i) != expectedOutput(i + 1),
        "Expected output has consecutive duplicate sequence of values")
    }

    // Log the output
    // scalastyle:off println
    println("Expected output, size = " + expectedOutput.size)
    println(expectedOutput.mkString("[", ",", "]"))
    println("Output, size = " + output.size)
    println(output.mkString("[", ",", "]"))
    // scalastyle:on println

    // Match the output with the expected output
    //将输出与预期输出相匹配
    output.foreach(o =>
      assert(expectedOutput.contains(o), "Expected value " + o + " not found")
    )
  }

  /** 
   *  Resets counter to prepare for the test
   *  准备测试 重置为计数
   *  */
  private def reset() {
    killed = false
    killCount = 0
    setupCalled = false
  }
}

/**
 * Thread to kill streaming context after a random period of time.
 * 线程在一个随机的时间后杀死流上下文
 * maxKillWaitTime:最大杀死等待的时间
 */
private[streaming]
class KillingThread(ssc: StreamingContext, maxKillWaitTime: Long) extends Thread with Logging {

  override def run() {
    try {
      // If it is the first killing, then allow the first checkpoint to be created
      //如果是第一次杀死,那么允许创建一个检查点
      //最小杀死等待时间
      var minKillWaitTime = if (MasterFailureTest.killCount == 0) 5000 else 2000
      //math.abs返回 double值的绝对值,如果参数是非负数,则返回该参数,如果参数是负数,则返回该参数的相反数
      val killWaitTime = minKillWaitTime + math.abs(Random.nextLong % maxKillWaitTime)
      logInfo("Kill wait time = " + killWaitTime)
      //死等待时间
      Thread.sleep(killWaitTime)
      logInfo(
        "\n---------------------------------------\n" +
          "Killing streaming context after " + killWaitTime + " ms" +
          "\n---------------------------------------\n"
      )
      if (ssc != null) {
        //StreamingContext暂停
        ssc.stop()
        MasterFailureTest.killed = true
        //死计数自增1
        MasterFailureTest.killCount += 1
      }
      logInfo("Killing thread finished normally")
    } catch {
      case ie: InterruptedException => logInfo("Killing thread interrupted")
      case e: Exception => logWarning("Exception in killing thread", e)
    }

  }
}


/**
 * Thread to generate input files periodically with the desired text.
 * 使用线程周期性生成输入inputSeq[String]所需文本的文件
 */
private[streaming]
class FileGeneratingThread(input: Seq[String], testDir: Path, interval: Long)
  extends Thread with Logging {

  override def run() {
    //创建本地临时目录
    val localTestDir = Utils.createTempDir()
    //println("createTempDir:"+localTestDir.toString())
    var fs = testDir.getFileSystem(new Configuration())
    //最大失败数
    val maxTries = 3
    try {
      //为了确保所有的流上下文已被设置
      Thread.sleep(5000) // To make sure that all the streaming context has been set up
      //println("input:"+input.size) //input:10
      for (i <- 0 until input.size) {
        // Write the data to a local file and then move it to the target test directory
        //将数据写入本地文件,然后将其移动到目标测试目录
        //C:\\Temp\spark-d7d82b5f-0484-40b5-8dce-b065ae504c1c\1
        val localFile = new File(localTestDir, (i + 1).toString)//因为从0开始所以+1
        //D:/checkpoint_test/581040b2-b8a4-4d5a-be73-8dfcb6b6516e/test/1
        val hadoopFile = new Path(testDir, (i + 1).toString)
        //D:/checkpoint_test/581040b2-b8a4-4d5a-be73-8dfcb6b6516e/test/.tmp_1
        val tempHadoopFile = new Path(testDir, ".tmp_" + (i + 1).toString)//因为从0开始所以+1
        //将数据写入本地临时目录文件 C:\\Temp\spark-d7d82b5f-0484-40b5-8dce-b065ae504c1c\1
        Files.write(input(i) + "\n", localFile, Charset.forName("UTF-8"))
        var tries = 0 //重试次数
        var done = false
            while (!done && tries < maxTries) {
              tries += 1
              try {
                // fs.copyFromLocalFile(new Path(localFile.toString), hadoopFile)
                //将本地临时目录文件,复制到临时的hadoop文件.tmp_1
                fs.copyFromLocalFile(new Path(localFile.toString), tempHadoopFile)
                //将hadoop文件.tmp_1重新命名/test/1
                fs.rename(tempHadoopFile, hadoopFile)
                //标记循环完成
            done = true
          } catch {
            case ioe: IOException => {
                  fs = testDir.getFileSystem(new Configuration())
                  logWarning("Attempt " + tries + " at generating file " + hadoopFile + " failed.",
                    ioe)
            }
          }
        }
        if (!done) {
          logError("Could not generate file " + hadoopFile)
        } else {
          logInfo("Generated file " + hadoopFile + " at " + System.currentTimeMillis)
        }
        Thread.sleep(interval)
        //删除临时文件
        localFile.delete()
      }
      logInfo("File generating thread finished normally")
    } catch {
      case ie: InterruptedException => logInfo("File generating thread interrupted")
      case e: Exception => logWarning("File generating in killing thread", e)
    } finally {
      fs.close()
      //递归删除本地临时目录
      Utils.deleteRecursively(localTestDir)
    }
  }
}
