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

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.storage.TaskResultBlockId
import org.apache.spark.TestUtils.JavaSourceFromString
import org.apache.spark.util.{MutableURLClassLoader, Utils}

/**
 * Removes the TaskResult from the BlockManager before delegating to a normal TaskResultGetter.
 * 从BlockManager删除TaskResult,删除之前给一个正常的taskresultgetter
 * Used to test the case where a BlockManager evicts the task result (or dies) before the
 * TaskResult is retrieved.
  * 用于测试BlockManager在检索到TaskResult之前排除任务结果(或死机)的情况
 */
class ResultDeletingTaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends TaskResultGetter(sparkEnv, scheduler) {
  var removedResult = false

  @volatile var removeBlockSuccessfully = false

  override def enqueueSuccessfulTask(
    taskSetManager: TaskSetManager, tid: Long, serializedData: ByteBuffer) {
    if (!removedResult) {
      // Only remove the result once, since we'd like to test the case where the task eventually
      // succeeds.
      //只删除一次结果,既然我们想测试的情况下,任务最终成功
      serializer.get().deserialize[TaskResult[_]](serializedData) match {
        case IndirectTaskResult(blockId, size) =>
          sparkEnv.blockManager.master.removeBlock(blockId)
          // removeBlock is asynchronous. Need to wait it's removed successfully
          //removeBlock是异步的,需要等待它成功删除
          try {
            eventually(timeout(3 seconds), interval(200 milliseconds)) {
              assert(!sparkEnv.blockManager.master.contains(blockId))
            }
            removeBlockSuccessfully = true
          } catch {
            case NonFatal(e) => removeBlockSuccessfully = false
          }
        case directResult: DirectTaskResult[_] =>
          taskSetManager.abort("Internal error: expect only indirect results")
      }
      serializedData.rewind()
      removedResult = true
    }
    super.enqueueSuccessfulTask(taskSetManager, tid, serializedData)
  }
}

/**
 * Tests related to handling task results (both direct and indirect).
 * 测试有关处理任务结果(直接和间接)
 */
class TaskResultGetterSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {

  // Set the Akka frame size to be as small as possible (it must be an integer, so 1 is as small
  // as we can make it) so the tests don't take too long.
   //以MB为单位的driver和executor之间通信信息的大小,设置值越大,driver可以接受越大的计算结果
  def conf: SparkConf = new SparkConf().set("spark.akka.frameSize", "1")
  test("handling results smaller than Akka frame size") {//处理结果小于Akka框架大小
    sc = new SparkContext("local", "test", conf)
    val result = sc.parallelize(Seq(1), 1).map(x => 2 * x).reduce((x, y) => x)
    assert(result === 2)
  }

  test("handling results larger than Akka frame size") {//处理结果大于Akka框架大小
    sc = new SparkContext("local", "test", conf)
    val akkaFrameSize =
      sc.env.actorSystem.settings.config.getBytes("akka.remote.netty.tcp.maximum-frame-size").toInt
    val result = sc.parallelize(Seq(1), 1).map(x => 1.to(akkaFrameSize).toArray).reduce((x, y) => x)
    assert(result === 1.to(akkaFrameSize).toArray)

    val RESULT_BLOCK_ID = TaskResultBlockId(0)
    assert(sc.env.blockManager.master.getLocations(RESULT_BLOCK_ID).size === 0,
      "Expect result to be removed from the block manager.")
  }

  test("task retried if result missing from block manager") {//任务重试,如果块管理器丢失结果
      // Set the maximum number of task failures to > 0, so that the task set isn't aborted
      // after the result is missing.
    //设置最大任务失败数,因此任务集不会在结果丢失后中止。
    sc = new SparkContext("local[1,2]", "test", conf)
    // If this test hangs, it's probably because no resource offers were made after the task
    // failed.
    //如果这个测试挂起,这可能是因为任务失败后没有提供资源
    val scheduler: TaskSchedulerImpl = sc.taskScheduler match {
      case taskScheduler: TaskSchedulerImpl =>
        taskScheduler
      case _ =>
        assert(false, "Expect local cluster to use TaskSchedulerImpl")
        throw new ClassCastException
    }
    val resultGetter = new ResultDeletingTaskResultGetter(sc.env, scheduler)
    scheduler.taskResultGetter = resultGetter
    val akkaFrameSize =
      sc.env.actorSystem.settings.config.getBytes("akka.remote.netty.tcp.maximum-frame-size").toInt
    val result = sc.parallelize(Seq(1), 1).map(x => 1.to(akkaFrameSize).toArray).reduce((x, y) => x)
    assert(resultGetter.removeBlockSuccessfully)
    assert(result === 1.to(akkaFrameSize).toArray)

    // Make sure two tasks were run (one failed one, and a second retried one).
    //确保两个任务运行(一个失败,第二重试一)
    assert(scheduler.nextTaskId.get() === 2)
  }

  /**
   * Make sure we are using the context classloader when deserializing failed TaskResults instead
   * of the Spark classloader.
   * 确保我们使用上下文类加载器在反序列化TaskResults而失败Spark类加载器
   *
   * This test compiles a jar containing an exception and tests that when it is thrown on the
   * executor, enqueueFailedTask can correctly deserialize the failure and identify the thrown
   * exception as the cause.
    *
    * 此测试编译包含异常的jar,并测试在执行器上抛出的值时,enqueueFailedTask可以正确地反序列化失败,并将引发的异常识别为原因
	 *
   * Before this fix, enqueueFailedTask would throw a ClassNotFoundException when deserializing
   * the exception, resulting in an UnknownReason for the TaskEndResult.
    * 在此修复之前,enqueueFailedTask在反序列化异常时会抛出ClassNotFoundException,从而导致TaskEndResult的UnknownReason
   */
  //反序列化的类加载器正确失败任务
  test("failed task deserialized with the correct classloader (SPARK-11195)") {
    // compile a small jar containing an exception that will be thrown on an executor.
    //编译一个jar包含一个执行器异常
    val tempDir = Utils.createTempDir()
    val srcDir = new File(tempDir, "repro/")
    srcDir.mkdirs()
    val excSource = new JavaSourceFromString(new File(srcDir, "MyException").getAbsolutePath,
      """package repro;
        |
        |public class MyException extends Exception {
        |}
      """.stripMargin)
    val excFile = TestUtils.createCompiledClass("MyException", srcDir, excSource, Seq.empty)
    val jarFile = new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    TestUtils.createJar(Seq(excFile), jarFile, directoryPrefix = Some("repro"))

    // ensure we reset the classloader after the test completes
    //确保我们的类加载器测试完成后复位
    val originalClassLoader = Thread.currentThread.getContextClassLoader
    try {
      // load the exception from the jar
      //加载jar抛出异常
      val loader = new MutableURLClassLoader(new Array[URL](0), originalClassLoader)
      loader.addURL(jarFile.toURI.toURL)
      Thread.currentThread().setContextClassLoader(loader)
      val excClass: Class[_] = Utils.classForName("repro.MyException")

      // NOTE: we must run the cluster with "local" so that the executor can load the compiled
      // jar.
      //我们必须以"本地"使执行器可以加载编译jar运行群集
      sc = new SparkContext("local", "test", conf)
      val rdd = sc.parallelize(Seq(1), 1).map { _ =>
        val exc = excClass.newInstance().asInstanceOf[Exception]
        throw exc
      }

      // the driver should not have any problems resolving the exception class and determining
      // why the task failed.
      //该驱动程序不应该有任何解决异常的问题,并确定任务失败的原因
      val exceptionMessage = intercept[SparkException] {
        rdd.collect()
      }.getMessage

      val expectedFailure = """(?s).*Lost task.*: repro.MyException.*""".r
      val unknownFailure = """(?s).*Lost task.*: UnknownReason.*""".r

      assert(expectedFailure.findFirstMatchIn(exceptionMessage).isDefined)
      assert(unknownFailure.findFirstMatchIn(exceptionMessage).isEmpty)
    } finally {
      Thread.currentThread.setContextClassLoader(originalClassLoader)
    }
  }
}

