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

package org.apache.spark.sql.execution.ui

import java.util.Properties

import org.apache.spark.{SparkException, SparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.execution.metric.LongSQLMetricValue
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.test.SharedSQLContext
//SQL的监听测试套件
class SQLListenerSuite extends SparkFunSuite with SharedSQLContext {
  import testImplicits._

  private def createTestDataFrame: DataFrame = {
    Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1")
  }
  //创建属性
  private def createProperties(executionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    properties
  }
  //创建阶段的信息
  private def createStageInfo(stageId: Int, attemptId: Int): StageInfo = new StageInfo(
    stageId = stageId,
    attemptId = attemptId,
    // The following fields are not used in tests
    //下列字段不在测试中使用
    name = "",
    numTasks = 0,
    rddInfos = Nil,
    parentIds = Nil,
    details = ""
  )
  //创建任务的信息
  private def createTaskInfo(taskId: Int, attemptNumber: Int): TaskInfo = new TaskInfo(
    taskId = taskId,
    attemptNumber = attemptNumber,
    // The following fields are not used in tests
    //下列字段不在测试中使用
    index = 0,
    launchTime = 0,
    executorId = "",
    host = "",
    taskLocality = null,
    speculative = false
  )
  //创建任务指标
  private def createTaskMetrics(accumulatorUpdates: Map[Long, Long]): TaskMetrics = {
    val metrics = new TaskMetrics
    metrics.setAccumulatorsUpdater(() => accumulatorUpdates.mapValues(new LongSQLMetricValue(_)))
    metrics.updateAccumulators()
    metrics
  }

  test("basic") {//基础
    val listener = new SQLListener(ctx)
    val executionId = 0
    val df = createTestDataFrame
    val accumulatorIds =
      SparkPlanGraph(df.queryExecution.executedPlan).nodes.flatMap(_.metrics.map(_.accumulatorId))
    // Assume all accumulators are long
      //假定所有的累加器为长整形
    var accumulatorValue = 0L
    val accumulatorUpdates = accumulatorIds.map { id =>
      accumulatorValue += 1L
      (id, accumulatorValue)
    }.toMap

    listener.onExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanGraph(df.queryExecution.executedPlan),
      System.currentTimeMillis())

    val executionUIData = listener.executionIdToData(0)

    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq(
        createStageInfo(0, 0),
        createStageInfo(1, 0)
      ),
      createProperties(executionId)))
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 0)))

    assert(listener.getExecutionMetrics(0).isEmpty)

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, metrics)
      //(任务ID,阶段标识,阶段尝试,度量)
      (0L, 0, 0, createTaskMetrics(accumulatorUpdates)),
      (1L, 0, 0, createTaskMetrics(accumulatorUpdates))
    )))

    assert(listener.getExecutionMetrics(0) === accumulatorUpdates.mapValues(_ * 2))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, metrics)
      //(任务ID,阶段标识,阶段尝试,度量)
      (0L, 0, 0, createTaskMetrics(accumulatorUpdates)),
      (1L, 0, 0, createTaskMetrics(accumulatorUpdates.mapValues(_ * 2)))
    )))

    assert(listener.getExecutionMetrics(0) === accumulatorUpdates.mapValues(_ * 3))

    // Retrying a stage should reset the metrics 再试阶段应该重新度量
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 1)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, metrics)
      //(任务ID,阶段标识,阶段尝试,度量)
      (0L, 0, 1, createTaskMetrics(accumulatorUpdates)),
      (1L, 0, 1, createTaskMetrics(accumulatorUpdates))
    )))

    assert(listener.getExecutionMetrics(0) === accumulatorUpdates.mapValues(_ * 2))

    // Ignore the task end for the first attempt 
    //忽略第一次尝试的任务结束
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 100))))

    assert(listener.getExecutionMetrics(0) === accumulatorUpdates.mapValues(_ * 2))

    // Finish two tasks
    //两个任务完成
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 2))))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 3))))

    assert(listener.getExecutionMetrics(0) === accumulatorUpdates.mapValues(_ * 5))

    // Summit a new stage
    //提交一个新Stage
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(1, 0)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, metrics)
      (0L, 1, 0, createTaskMetrics(accumulatorUpdates)),
      (1L, 1, 0, createTaskMetrics(accumulatorUpdates))
    )))

    assert(listener.getExecutionMetrics(0) === accumulatorUpdates.mapValues(_ * 7))

    // Finish two tasks
    //完成两个任务
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 3))))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 3))))

    assert(listener.getExecutionMetrics(0) === accumulatorUpdates.mapValues(_ * 11))

    assert(executionUIData.runningJobs === Seq(0))
    assert(executionUIData.succeededJobs.isEmpty)
    assert(executionUIData.failedJobs.isEmpty)

    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))
    listener.onExecutionEnd(executionId, System.currentTimeMillis())

    assert(executionUIData.runningJobs.isEmpty)
    assert(executionUIData.succeededJobs === Seq(0))
    assert(executionUIData.failedJobs.isEmpty)

    assert(listener.getExecutionMetrics(0) === accumulatorUpdates.mapValues(_ * 11))
  }

  test("onExecutionEnd happens before onJobEnd(JobSucceeded)") {
    val listener = new SQLListener(ctx)
    val executionId = 0
    val df = createTestDataFrame
    listener.onExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanGraph(df.queryExecution.executedPlan),
      System.currentTimeMillis())
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onExecutionEnd(executionId, System.currentTimeMillis())
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    val executionUIData = listener.executionIdToData(0)
    assert(executionUIData.runningJobs.isEmpty)
    assert(executionUIData.succeededJobs === Seq(0))
    assert(executionUIData.failedJobs.isEmpty)
  }

  test("onExecutionEnd happens before multiple onJobEnd(JobSucceeded)s") {
    val listener = new SQLListener(ctx)
    val executionId = 0
    val df = createTestDataFrame
    listener.onExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanGraph(df.queryExecution.executedPlan),
      System.currentTimeMillis())
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onJobEnd(SparkListenerJobEnd(
        jobId = 0,
        time = System.currentTimeMillis(),
        JobSucceeded
    ))

    listener.onJobStart(SparkListenerJobStart(
      jobId = 1,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onExecutionEnd(executionId, System.currentTimeMillis())
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 1,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    val executionUIData = listener.executionIdToData(0)
    assert(executionUIData.runningJobs.isEmpty)
    assert(executionUIData.succeededJobs.sorted === Seq(0, 1))
    assert(executionUIData.failedJobs.isEmpty)
  }

  test("onExecutionEnd happens before onJobEnd(JobFailed)") {
    val listener = new SQLListener(ctx)
    val executionId = 0
    val df = createTestDataFrame
    listener.onExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanGraph(df.queryExecution.executedPlan),
      System.currentTimeMillis())
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq.empty,
      createProperties(executionId)))
    listener.onExecutionEnd(executionId, System.currentTimeMillis())
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobFailed(new RuntimeException("Oops"))
    ))

    val executionUIData = listener.executionIdToData(0)
    assert(executionUIData.runningJobs.isEmpty)
    assert(executionUIData.succeededJobs.isEmpty)
    assert(executionUIData.failedJobs === Seq(0))
  }

  test("SPARK-11126: no memory leak when running non SQL jobs") {
    val previousStageNumber = sqlContext.listener.stageIdToStageMetrics.size
    sqlContext.sparkContext.parallelize(1 to 10).foreach(i => ())
    sqlContext.sparkContext.listenerBus.waitUntilEmpty(10000)
    // listener should ignore the non SQL stage
    assert(sqlContext.listener.stageIdToStageMetrics.size == previousStageNumber)

    sqlContext.sparkContext.parallelize(1 to 10).toDF().foreach(i => ())
    sqlContext.sparkContext.listenerBus.waitUntilEmpty(10000)
    // listener should save the SQL stage
    //监听应该保存SQL阶段
    assert(sqlContext.listener.stageIdToStageMetrics.size == previousStageNumber + 1)
  }

}
//SQL监听内存泄漏套件
class SQLListenerMemoryLeakSuite extends SparkFunSuite {

  test("no memory leak") {//没有内存泄漏
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      //不要重试任务快速运行此测试
      .set("spark.task.maxFailures", "1") // Don't retry the tasks to run this test quickly
      //将它设置为50快速运行此测试
      .set("spark.sql.ui.retainedExecutions", "50") // Set it to 50 to run this test quickly
    val sc = new SparkContext(conf)
    try {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      // Run 100 successful executions and 100 failed executions.
      //运行100个成功的执行和100个失败的执行。
      // Each execution only has one job and one stage.
      //每一个执行只有一个工作和一个阶段
      for (i <- 0 until 100) {
        val df = Seq(
          (1, 1),
          (2, 2)
        ).toDF()
        df.collect()
        try {
          df.foreach(_ => throw new RuntimeException("Oops"))
        } catch {
          //这是一个失败的工作
          case e: SparkException => // This is expected for a failed job
        }
      }
      sc.listenerBus.waitUntilEmpty(10000)
      assert(sqlContext.listener.getCompletedExecutions.size <= 50)
      assert(sqlContext.listener.getFailedExecutions.size <= 50)
      // 50 for successful executions and 50 for failed executions
      //50成功的处决和50的失败的处决
      assert(sqlContext.listener.executionIdToData.size <= 100)
      assert(sqlContext.listener.jobIdToExecutionId.size <= 100)
      assert(sqlContext.listener.stageIdToStageMetrics.size <= 100)
    } finally {
      sc.stop()
    }
  }
}
