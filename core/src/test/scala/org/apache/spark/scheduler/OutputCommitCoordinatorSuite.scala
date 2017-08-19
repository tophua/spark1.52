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
import java.util.concurrent.TimeoutException

import org.mockito.Matchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.hadoop.mapred.{TaskAttemptID, JobConf, TaskAttemptContext, OutputCommitter}

import org.apache.spark._
import org.apache.spark.rdd.{RDD, FakeOutputCommitter}
import org.apache.spark.util.Utils

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Unit tests for the output commit coordination functionality.
 * 输出提交协调功能的单元测试
 * The unit test makes both the original task and the speculated task
 * attempt to commit, where committing is emulated by creating a
 * directory. If both tasks create directories then the end result is
 * a failure.
  *
 * 单元测试使原始任务和推测任务都尝试提交,其中通过创建目录来模拟提交,
  * 如果两个任务都创建目录,那么最终的结果就是失败。
  *
 * Note that there are some aspects of this test that are less than ideal.
 * In particular, the test mocks the speculation-dequeuing logic to always
 * dequeue a task and consider it as speculated. Immediately after initially
 * submitting the tasks and calling reviveOffers(), reviveOffers() is invoked
 * again to pick up the speculated task. This may be hacking the original
 * behavior in too much of an unrealistic fashion.
  *
  * 请注意，该测试的一些方面不太理想,特别地,测试模拟推测出队逻辑以总是出现任务,并将其视为推测,
  * 在最初提交任务并调用reviveOffers（）之后,立即调用reviveOffers（）来接收推测的任务,
  * 这可能是以太多的不切实际的方式来侵蚀原来的行为
 *
 * Also, the validation is done by checking the number of files in a directory.
 * Ideally, an accumulator would be used for this, where we could increment
 * the accumulator in the output committer's commitTask() call. If the call to
 * commitTask() was called twice erroneously then the test would ideally fail because
 * the accumulator would be incremented twice.
 *
  * 此外,通过检查目录中的文件数量来完成验证。对于此,将使用累加器,我们可以在输出提交者的commitTask()调用中增加累加器,
  * 如果对commitTask()的调用被错误地调用两次,那么测试将理想地失败,因为累加器将增加两次,
  *
 * The problem with this test implementation is that when both a speculated task and
 * its original counterpart complete, only one of the accumulator's increments is
 * captured. This results in a paradox where if the OutputCommitCoordinator logic
 * was not in SparkHadoopWriter, the tests would still pass because only one of the
 * increments would be captured even though the commit in both tasks was executed
 * erroneously.
  *
  * 此测试实现的问题是,当推测的任务和其原始对应完成时,仅捕获累加器的一个增量,
  * 这导致一个悖论,如果OutputCommitCoordinator逻辑不在SparkHadoopWriter中,则测试仍然会通过,
  * 因为即使两个任务中的提交都被错误地执行,也只能捕获一个增量。
  *
 * See also: [[OutputCommitCoordinatorIntegrationSuite]] for integration tests that do
 * not use mocks.
 */
class OutputCommitCoordinatorSuite extends SparkFunSuite with BeforeAndAfter {

  var outputCommitCoordinator: OutputCommitCoordinator = null
  var tempDir: File = null
  var sc: SparkContext = null

  before {
    tempDir = Utils.createTempDir()
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName(classOf[OutputCommitCoordinatorSuite].getSimpleName)
      .set("spark.speculation", "true")//设定是否使用推测执行机制
    sc = new SparkContext(conf) {
      override private[spark] def createSparkEnv(
          conf: SparkConf,
          isLocal: Boolean,
          listenerBus: LiveListenerBus): SparkEnv = {
        outputCommitCoordinator = spy(new OutputCommitCoordinator(conf, isDriver = true))
        // Use Mockito.spy() to maintain the default infrastructure everywhere else.
        // This mocking allows us to control the coordinator responses in test cases.
        //使用Mockito.spy()来维护其他地方的默认基础设施,这个嘲弄允许我们在测试用例中控制协调器响应
        SparkEnv.createDriverEnv(conf, isLocal, listenerBus,
          SparkContext.numDriverCores(master), Some(outputCommitCoordinator))
      }
    }
    // Use Mockito.spy() to maintain the default infrastructure everywhere else
    //使用Mockito.spy（）来维护其他地方的默认基础架构
    val mockTaskScheduler = spy(sc.taskScheduler.asInstanceOf[TaskSchedulerImpl])

    doAnswer(new Answer[Unit]() {
      override def answer(invoke: InvocationOnMock): Unit = {
        // Submit the tasks, then force the task scheduler to dequeue the
        // speculated task
        //提交任务,然后强制任务调度程序对推测的任务进行排队
        invoke.callRealMethod()
        mockTaskScheduler.backend.reviveOffers()
      }
    }).when(mockTaskScheduler).submitTasks(Matchers.any())

    doAnswer(new Answer[TaskSetManager]() {
      override def answer(invoke: InvocationOnMock): TaskSetManager = {
        val taskSet = invoke.getArguments()(0).asInstanceOf[TaskSet]
        new TaskSetManager(mockTaskScheduler, taskSet, 4) {
          var hasDequeuedSpeculatedTask = false
          override def dequeueSpeculativeTask(
              execId: String,
              host: String,
              locality: TaskLocality.Value): Option[(Int, TaskLocality.Value)] = {
            if (!hasDequeuedSpeculatedTask) {
              hasDequeuedSpeculatedTask = true
              Some(0, TaskLocality.PROCESS_LOCAL)
            } else {
              None
            }
          }
        }
      }
    }).when(mockTaskScheduler).createTaskSetManager(Matchers.any(), Matchers.any())

    sc.taskScheduler = mockTaskScheduler
    val dagSchedulerWithMockTaskScheduler = new DAGScheduler(sc, mockTaskScheduler)
    sc.taskScheduler.setDAGScheduler(dagSchedulerWithMockTaskScheduler)
    sc.dagScheduler = dagSchedulerWithMockTaskScheduler
  }

  after {
    sc.stop()
    tempDir.delete()
    outputCommitCoordinator = null
  }
  //只有两个重复提交任务之一应该提交
  test("Only one of two duplicate commit tasks should commit") {
    val rdd = sc.parallelize(Seq(1), 1)
    sc.runJob(rdd, OutputCommitFunctions(tempDir.getAbsolutePath).commitSuccessfully _,
      0 until rdd.partitions.size)
    assert(tempDir.list().size === 1)
  }
  //如果commit失败，如果任务被重试，它不应被锁定，并且将成功
  test("If commit fails, if task is retried it should not be locked, and will succeed.") {
    val rdd = sc.parallelize(Seq(1), 1)
    sc.runJob(rdd, OutputCommitFunctions(tempDir.getAbsolutePath).failFirstCommitAttempt _,
      0 until rdd.partitions.size)
    assert(tempDir.list().size === 1)
  }
  //如果所有提交被拒绝，作业都不应该完成
  test("Job should not complete if all commits are denied") {
    // Create a mock OutputCommitCoordinator that denies all attempts to commit
    // 创建一个模拟OutputCommitCoordinator，拒绝所有尝试提交
    doReturn(false).when(outputCommitCoordinator).handleAskPermissionToCommit(
      Matchers.any(), Matchers.any(), Matchers.any())
    val rdd: RDD[Int] = sc.parallelize(Seq(1), 1)
    def resultHandler(x: Int, y: Unit): Unit = {}
    val futureAction: SimpleFutureAction[Unit] = sc.submitJob[Int, Unit, Unit](rdd,
      OutputCommitFunctions(tempDir.getAbsolutePath).commitSuccessfully,
      0 until rdd.partitions.size, resultHandler, () => Unit)
    // It's an error if the job completes successfully even though no committer was authorized,
    // so throw an exception if the job was allowed to complete.
    //即使没有提交者被授权,作业成功完成也是一个错误,因此如果作业被允许完成,则抛出异常。
    intercept[TimeoutException] {
    //Await.result或者Await.ready会导致当前线程被阻塞,并等待actor通过它的应答来完成Future
      Await.result(futureAction, 5 seconds)
    }
    assert(tempDir.list().size === 0)
  }
  //只有授权的提交者失败才能清除授权的提交者锁定
  test("Only authorized committer failures can clear the authorized committer lock (SPARK-6614)") {
    val stage: Int = 1
    val partition: Int = 2
    val authorizedCommitter: Int = 3
    val nonAuthorizedCommitter: Int = 100
    outputCommitCoordinator.stageStart(stage)

    assert(outputCommitCoordinator.canCommit(stage, partition, authorizedCommitter))
    assert(!outputCommitCoordinator.canCommit(stage, partition, nonAuthorizedCommitter))
    // The non-authorized committer fails
    //未授权的提交程序失败
    outputCommitCoordinator.taskCompleted(
      stage, partition, attemptNumber = nonAuthorizedCommitter, reason = TaskKilled)
    // New tasks should still not be able to commit because the authorized committer has not failed
    // 新任务仍然无法提交，因为授权的提交者没有失败
    assert(
      !outputCommitCoordinator.canCommit(stage, partition, nonAuthorizedCommitter + 1))
    // The authorized committer now fails, clearing the lock
    //授权的提交者现在失败，清除锁定
    outputCommitCoordinator.taskCompleted(
      stage, partition, attemptNumber = authorizedCommitter, reason = TaskKilled)
    // A new task should now be allowed to become the authorized committer
    //现在应该允许一个新的任务成为授权的提交者
    assert(
      outputCommitCoordinator.canCommit(stage, partition, nonAuthorizedCommitter + 2))
    // There can only be one authorized committer
    //只能有一个授权的提交者
    assert(
      !outputCommitCoordinator.canCommit(stage, partition, nonAuthorizedCommitter + 3))
  }
}

/**
 * Class with methods that can be passed to runJob to test commits with a mock committer.
  * 使用可以传递给runJob的方法来测试使用模拟提交者的提交的类。
 */
private case class OutputCommitFunctions(tempDirPath: String) {

  // Mock output committer that simulates a successful commit (after commit is authorized)
  //模拟成功提交的模拟输出提交者（提交授权后）
  private def successfulOutputCommitter = new FakeOutputCommitter {
    override def commitTask(context: TaskAttemptContext): Unit = {
      Utils.createDirectory(tempDirPath)
    }
  }

  // Mock output committer that simulates a failed commit (after commit is authorized)
  // 模拟一个失败的提交的模拟输出提交者（授权后提交）
  private def failingOutputCommitter = new FakeOutputCommitter {
    override def commitTask(taskAttemptContext: TaskAttemptContext) {
      throw new RuntimeException
    }
  }

  def commitSuccessfully(iter: Iterator[Int]): Unit = {
    val ctx = TaskContext.get()
    runCommitWithProvidedCommitter(ctx, iter, successfulOutputCommitter)
  }

  def failFirstCommitAttempt(iter: Iterator[Int]): Unit = {
    val ctx = TaskContext.get()
    runCommitWithProvidedCommitter(ctx, iter,
      if (ctx.attemptNumber == 0) failingOutputCommitter else successfulOutputCommitter)
  }

  private def runCommitWithProvidedCommitter(
      ctx: TaskContext,
      iter: Iterator[Int],
      outputCommitter: OutputCommitter): Unit = {
    def jobConf = new JobConf {
      override def getOutputCommitter(): OutputCommitter = outputCommitter
    }
    val sparkHadoopWriter = new SparkHadoopWriter(jobConf) {
      override def newTaskAttemptContext(
        conf: JobConf,
        attemptId: TaskAttemptID): TaskAttemptContext = {
        mock(classOf[TaskAttemptContext])
      }
    }
    sparkHadoopWriter.setup(ctx.stageId, ctx.partitionId, ctx.attemptNumber)
    sparkHadoopWriter.commit()
  }
}
