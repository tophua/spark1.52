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


private[streaming]
object MasterFailureTest extends Logging {

  @volatile var killed = false
  @volatile var killCount = 0
  @volatile var setupCalled = false

  def main(args: Array[String]) {
    // scalastyle:off println
    //批量大小以毫秒为单位
    if (args.size < 2) {
      println(
        "Usage: MasterFailureTest <local/HDFS directory> <# batches> " +
          "[<batch size in milliseconds>]")
      System.exit(1)
    }
    //检查点
    val directory = args(0)
    //批量时间
    val numBatches = args(1).toInt//批量
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
    val input = (1 to numBatches).map(_.toString).toSeq
    // Expected output: time=1 ==> [ 1 ] , time=2 ==> [ 2 ] , time=3 ==> [ 3 ] , ...
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
      st.flatMap(_.split(" "))
        .map(x => (x, 1L))
        .updateStateByKey[Long](updateFunc)
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
   * 测试与多个主故障的流操作,并验证最后一组输出值是否为预期的或不
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
    //为这个测试创建目录
    val uuid = UUID.randomUUID().toString
    val rootDir = new Path(directory, uuid)
    val fs = rootDir.getFileSystem(new Configuration())
    val checkpointDir = new Path(rootDir, "checkpoint")
    val testDir = new Path(rootDir, "test")
    fs.mkdirs(checkpointDir)
    fs.mkdirs(testDir)

    // Setup the stream computation with the given operation
    //用给定的操作设置流计算
    val ssc = StreamingContext.getOrCreate(checkpointDir.toString, () => {
      setupStreams(batchDuration, operation, checkpointDir, testDir)
    })

    // Check if setupStream was called to create StreamingContext
    //检查setupstream调用创建StreamingContext,并不是从检查点文件创建的
    // (and not created from checkpoint file)
    assert(setupCalled, "Setup was not called in the first call to StreamingContext.getOrCreate")

    // Start generating files in the a different thread
    // 在一个不同的线程中开始生成文件
    val fileGeneratingThread = new FileGeneratingThread(input, testDir, batchDuration.milliseconds)
    fileGeneratingThread.start()

    // Run the streams and repeatedly kill it until the last expected output
    // has been generated, or until it has run for twice the expected time
    //运行流，并多次杀死它，直到最后一个预期的输出已产生,或者直到它运行了两倍的预期时间
    val lastExpectedOutput = expectedOutput.last
    val maxTimeToRun = expectedOutput.size * batchDuration.milliseconds * 2
    val mergedOutput = runStreams(ssc, lastExpectedOutput, maxTimeToRun)

    fileGeneratingThread.join()
    fs.delete(checkpointDir, true)
    fs.delete(testDir, true)
    logInfo("Finished test after " + killCount + " failures")
    mergedOutput
  }

  /**
   * Sets up the stream computation with the given operation, directory (local or HDFS),
   * and batch duration. Returns the streaming context and the directory to which
   * files should be written for testing.
   * 用给定的操作设置流计算,目录(本地或HDFS)和间隔时间,返回流上下文和文件写入测试的目录,   
   */
  private def setupStreams[T: ClassTag](
      batchDuration: Duration,
      operation: DStream[String] => DStream[T],
      checkpointDir: Path,
      testDir: Path
    ): StreamingContext = {
    // Mark that setup was called
    // 标记设置被调用
    setupCalled = true

    // Setup the streaming computation with the given operation
    //设置给定的流式计算操作
    val ssc = new StreamingContext("local[4]", "MasterFailureTest", batchDuration, null, Nil,
      Map())
    ssc.checkpoint(checkpointDir.toString)
    val inputStream = ssc.textFileStream(testDir.toString)
    val operatedStream = operation(inputStream)
    val outputStream = new TestOutputStream(operatedStream)
    outputStream.register()
    ssc
  }


  /**
   * Repeatedly starts and kills the streaming context until timed out or
   * the last expected output is generated. Finally, return
   * 重复启动和杀死流上下文,直到超时或生成最后的期望输出
   */
  private def runStreams[T: ClassTag](
      ssc_ : StreamingContext,
      lastExpectedOutput: T,
      maxTimeToRun: Long
   ): Seq[T] = {

    var ssc = ssc_
    var totalTimeRan = 0L
    var isLastOutputGenerated = false
    var isTimedOut = false
    val mergedOutput = new ArrayBuffer[T]()
    val checkpointDir = ssc.checkpointDir
    val batchDuration = ssc.graph.batchDuration

    while(!isLastOutputGenerated && !isTimedOut) {
      // Get the output buffer
      //获取输出缓冲区
      val outputBuffer = ssc.graph.getOutputStreams().head.asInstanceOf[TestOutputStream[T]].output
      def output = outputBuffer.flatMap(x => x)

      // Start the thread to kill the streaming after some time
      // 在一段时间后开始线程杀死流
      killed = false
      val killingThread = new KillingThread(ssc, batchDuration.milliseconds * 10)
      killingThread.start()

      var timeRan = 0L
      try {
        // Start the streaming computation and let it run while ...
        //开始流式计算,并让它运行
        // (i) StreamingContext has not been shut down yet
        // (ii) The last expected output has not been generated yet
        // (iii) Its not timed out yet
        System.clearProperty("spark.streaming.clock")
        System.clearProperty("spark.driver.port")
        ssc.start()
        val startTime = System.currentTimeMillis()
        while (!killed && !isLastOutputGenerated && !isTimedOut) {
          Thread.sleep(100)
          timeRan = System.currentTimeMillis() - startTime
          isLastOutputGenerated = (output.nonEmpty && output.last == lastExpectedOutput)
          isTimedOut = (timeRan + totalTimeRan > maxTimeToRun)
        }
      } catch {
        case e: Exception => logError("Error running streaming context", e)
      }
      if (killingThread.isAlive) {
        killingThread.interrupt()
        // SparkContext.stop will set SparkEnv.env to null. We need to make sure SparkContext is
        // stopped before running the next test. Otherwise, it's possible that we set SparkEnv.env
        // to null after the next test creates the new SparkContext and fail the test.
        killingThread.join()
      }
      ssc.stop()

      logInfo("Has been killed = " + killed)
      logInfo("Is last output generated = " + isLastOutputGenerated)
      logInfo("Is timed out = " + isTimedOut)

      // Verify whether the output of each batch has only one element or no element
      //检查每个批处理的输出是否只有一个元素或没有元素
      // and then merge the new output with all the earlier output
      //然后合并新的输出与所有之前的输出
      mergedOutput ++= output
      totalTimeRan += timeRan
      logInfo("New output = " + output)
      logInfo("Merged output = " + mergedOutput)
      logInfo("Time ran = " + timeRan)
      logInfo("Total time ran = " + totalTimeRan)

      if (!isLastOutputGenerated && !isTimedOut) {
        val sleepTime = Random.nextInt(batchDuration.milliseconds.toInt * 10)
        logInfo(
          "\n-------------------------------------------\n" +
            "   Restarting stream computation in " + sleepTime + " ms   " +
            "\n-------------------------------------------\n"
        )
        Thread.sleep(sleepTime)
        // Recreate the streaming context from checkpoint
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
   *  重置为计数准备测试 
   *  */
  private def reset() {
    killed = false
    killCount = 0
    setupCalled = false
  }
}

/**
 * Thread to kill streaming context after a random period of time.
 * 线程在一个随机的时间后杀死流
 */
private[streaming]
class KillingThread(ssc: StreamingContext, maxKillWaitTime: Long) extends Thread with Logging {

  override def run() {
    try {
      // If it is the first killing, then allow the first checkpoint to be created
      var minKillWaitTime = if (MasterFailureTest.killCount == 0) 5000 else 2000
      val killWaitTime = minKillWaitTime + math.abs(Random.nextLong % maxKillWaitTime)
      logInfo("Kill wait time = " + killWaitTime)
      Thread.sleep(killWaitTime)
      logInfo(
        "\n---------------------------------------\n" +
          "Killing streaming context after " + killWaitTime + " ms" +
          "\n---------------------------------------\n"
      )
      if (ssc != null) {
        ssc.stop()
        MasterFailureTest.killed = true
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
 * 线程来周期性地生成与所需的文本的输入文件。
 */
private[streaming]
class FileGeneratingThread(input: Seq[String], testDir: Path, interval: Long)
  extends Thread with Logging {

  override def run() {
    val localTestDir = Utils.createTempDir()
    var fs = testDir.getFileSystem(new Configuration())
    val maxTries = 3
    try {
      //为了确保所有的流上下文已被设置
      Thread.sleep(5000) // To make sure that all the streaming context has been set up
      for (i <- 0 until input.size) {
        // Write the data to a local file and then move it to the target test directory
        //将数据写入本地文件,然后将其移动到目标测试目录
        val localFile = new File(localTestDir, (i + 1).toString)
        val hadoopFile = new Path(testDir, (i + 1).toString)
        val tempHadoopFile = new Path(testDir, ".tmp_" + (i + 1).toString)
        Files.write(input(i) + "\n", localFile, Charset.forName("UTF-8"))
        var tries = 0
        var done = false
            while (!done && tries < maxTries) {
              tries += 1
              try {
                // fs.copyFromLocalFile(new Path(localFile.toString), hadoopFile)
                fs.copyFromLocalFile(new Path(localFile.toString), tempHadoopFile)
                fs.rename(tempHadoopFile, hadoopFile)
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
        localFile.delete()
      }
      logInfo("File generating thread finished normally")
    } catch {
      case ie: InterruptedException => logInfo("File generating thread interrupted")
      case e: Exception => logWarning("File generating in killing thread", e)
    } finally {
      fs.close()
      Utils.deleteRecursively(localTestDir)
    }
  }
}
