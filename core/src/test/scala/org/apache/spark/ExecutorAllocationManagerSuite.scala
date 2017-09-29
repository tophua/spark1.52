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

package org.apache.spark

import scala.collection.mutable

import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.util.ManualClock

/**
 * Test add and remove behavior of ExecutorAllocationManager.
 * 创建和启动Executor分配管理器
 */
class ExecutorAllocationManagerSuite
  extends SparkFunSuite
  with LocalSparkContext
  with BeforeAndAfter {

  import ExecutorAllocationManager._
  import ExecutorAllocationManagerSuite._

  private val contexts = new mutable.ListBuffer[SparkContext]()

  before {
    contexts.clear()
  }

  after {
    contexts.foreach(_.stop())
  }

  test("verify min/max executors") {//验证最小/最大的执行者
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.testing", "true")
    val sc0 = new SparkContext(conf)
    contexts += sc0
    //动态分配是否已定义
    assert(sc0.executorAllocationManager.isDefined)
    sc0.stop()

    // Min < 0
    val conf1 = conf.clone().set("spark.dynamicAllocation.minExecutors", "-1")
    intercept[SparkException] { contexts += new SparkContext(conf1) }

    // Max < 0
    val conf2 = conf.clone().set("spark.dynamicAllocation.maxExecutors", "-1")
    intercept[SparkException] { contexts += new SparkContext(conf2) }

    // Both min and max, but min > max
    //都是最小值和最大值,但最小值大于最大值
    intercept[SparkException] { createSparkContext(2, 1) }

    // Both min and max, and min == max
    //最小值==最大值
    val sc1 = createSparkContext(1, 1)
    assert(sc1.executorAllocationManager.isDefined)
    sc1.stop()

    // Both min and max, and min < max
    //最小值小于最大值
    val sc2 = createSparkContext(1, 2)
    assert(sc2.executorAllocationManager.isDefined)
    sc2.stop()
  }

  test("starting state") {//启动状态
    sc = createSparkContext()
    val manager:ExecutorAllocationManager = sc.executorAllocationManager.get
    assert(numExecutorsTarget(manager) === 1)
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(executorIds(manager).isEmpty)
    assert(addTime(manager) === ExecutorAllocationManager.NOT_SET)
    assert(removeTimes(manager).isEmpty)
  }

  test("add executors") {//添加执行
    sc = createSparkContext(1, 10, 1)//最小1,最大10,初始化1
    val manager = sc.executorAllocationManager.get
    //
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Keep adding until the limit is reached
    //继续添加,直到达到极限为止
    //所需的执行数 initialExecutors
    assert(numExecutorsTarget(manager) === 1)
    //增加的执行数
    assert(numExecutorsToAdd(manager) === 1)
    //从集群管理器请求的执行者
    assert(addExecutors(manager) === 1)//添加执行者数
    //所需的执行数
    assert(numExecutorsTarget(manager) === 2)
    //增加的执行数
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)//添加执行者数
    //所需的执行数
    assert(numExecutorsTarget(manager) === 4)
    //增加的执行数
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 4)//添加执行者数
    assert(numExecutorsTarget(manager) === 8)
    assert(numExecutorsToAdd(manager) === 8)
    //从集群管理器请求的执行者
    assert(addExecutors(manager) === 2) // reached the limit of 10 达到了10的极限
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)
    //从集群管理器请求的执行者
    assert(addExecutors(manager) === 0)
    //所需的执行数
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)

    // Register previously requested executors
    //注册之前的执行者
    onExecutorAdded(manager, "first")
    //所需的执行数
    assert(numExecutorsTarget(manager) === 10)
    onExecutorAdded(manager, "second")
    onExecutorAdded(manager, "third")
    onExecutorAdded(manager, "fourth")
    //所需的执行数
    assert(numExecutorsTarget(manager) === 10)
    //重复不应计数
    onExecutorAdded(manager, "first") // duplicates should not count
    onExecutorAdded(manager, "second")
    //所需的执行数
    assert(numExecutorsTarget(manager) === 10)

    // Try adding again 试着再添加
    // This should still fail because the number pending + running is still at the limit
    //这应该仍然失败,因为挂起的+运行的数量仍然在限制
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)
  }
  //添加上限待定任务执行者
  test("add executors capped by num pending tasks") {
    sc = createSparkContext(0, 10, 0)//最小0,最大10,初始化0
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 5)))

    // Verify that we're capped at number of tasks in the stage
    //确认在阶段的任务数量上限
    assert(numExecutorsTarget(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 3)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 5)
    assert(numExecutorsToAdd(manager) === 1)

    //Verify that running a task doesn't affect the target
    //验证正在运行的任务不会影响目标
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, 3)))
    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    sc.listenerBus.postToAll(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0, "executor-1")))
    assert(numExecutorsTarget(manager) === 5)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 6)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 8)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 8)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that re-running a task doesn't blow things up
    //确认重新运行一个任务,不会破坏别的任何东西
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, 3)))
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(1, 0, "executor-1")))
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 9)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that running a task once we're at our limit doesn't blow things up
    //验证一旦处于极限状态,运行任务就不会造成任何影响
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(0, 1, "executor-1")))
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 10)
  }

  test("cancel pending executors when no longer needed") {//取消挂起的执行者,当不再需要
    sc = createSparkContext(0, 10, 0)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, 5)))

    assert(numExecutorsTarget(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 3)

    val task1Info = createTaskInfo(0, 0, "executor-1")
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, task1Info))

    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 2)

    val task2Info = createTaskInfo(1, 0, "executor-1")
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, task2Info))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(2, 0, null, Success, task1Info, null))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(2, 0, null, Success, task2Info, null))

    assert(adjustRequestedExecutors(manager) === -1)
  }

  test("remove executors") {//删除执行者
    sc = createSparkContext(5, 10, 5)
    val manager = sc.executorAllocationManager.get
    (1 to 10).map(_.toString).foreach { id => onExecutorAdded(manager, id) }

    // Keep removing until the limit is reached
    //保持删除直到达到极限为止
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutor(manager, "1"))
    assert(executorsPendingToRemove(manager).size === 1)
    assert(executorsPendingToRemove(manager).contains("1"))
    assert(removeExecutor(manager, "2"))
    assert(removeExecutor(manager, "3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(executorsPendingToRemove(manager).contains("2"))
    assert(executorsPendingToRemove(manager).contains("3"))
    //删除不存在的执行者
    assert(!removeExecutor(manager, "100")) // remove non-existent executors
    assert(!removeExecutor(manager, "101"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(removeExecutor(manager, "4"))
    assert(removeExecutor(manager, "5"))
    //达到了5的限制
    assert(!removeExecutor(manager, "6")) // reached the limit of 5
    assert(executorsPendingToRemove(manager).size === 5)
    assert(executorsPendingToRemove(manager).contains("4"))
    assert(executorsPendingToRemove(manager).contains("5"))
    assert(!executorsPendingToRemove(manager).contains("6"))

    // Kill executors previously requested to remove
    //杀死之前要求删除
    onExecutorRemoved(manager, "1")
    assert(executorsPendingToRemove(manager).size === 4)
    assert(!executorsPendingToRemove(manager).contains("1"))
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")
    assert(executorsPendingToRemove(manager).size === 2)
    assert(!executorsPendingToRemove(manager).contains("2"))
    assert(!executorsPendingToRemove(manager).contains("3"))
    onExecutorRemoved(manager, "2") // duplicates should not count 重复不应计数
    onExecutorRemoved(manager, "3")
    assert(executorsPendingToRemove(manager).size === 2)
    onExecutorRemoved(manager, "4")
    onExecutorRemoved(manager, "5")
    assert(executorsPendingToRemove(manager).isEmpty)

    // Try removing again 尝试删除
    // This should still fail because the number pending + running is still at the limit
    //应该仍然失败,因为挂起的+运行的数量仍然在限制
    assert(!removeExecutor(manager, "7"))
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(!removeExecutor(manager, "8"))
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test ("interleaving add and remove") {//添加和删除
    sc = createSparkContext(5, 10, 5)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Add a few executors 添加一些执行者
    assert(addExecutors(manager) === 1)
    assert(addExecutors(manager) === 2)
    onExecutorAdded(manager, "1")
    onExecutorAdded(manager, "2")
    onExecutorAdded(manager, "3")
    onExecutorAdded(manager, "4")
    onExecutorAdded(manager, "5")
    onExecutorAdded(manager, "6")
    onExecutorAdded(manager, "7")
    onExecutorAdded(manager, "8")
    assert(executorIds(manager).size === 8)

    // Remove until limit 删除到限制
    assert(removeExecutor(manager, "1"))
    assert(removeExecutor(manager, "2"))
    assert(removeExecutor(manager, "3"))
    assert(!removeExecutor(manager, "4")) // lower limit reached 下限达到
    assert(!removeExecutor(manager, "5"))
    onExecutorRemoved(manager, "1")
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")
    assert(executorIds(manager).size === 5)

    // Add until limit 添加到极限
    assert(addExecutors(manager) === 2) // upper limit reached 上限达到
    assert(addExecutors(manager) === 0)
    assert(!removeExecutor(manager, "4")) // still at lower limit 较低的限度
    assert(!removeExecutor(manager, "5"))
    onExecutorAdded(manager, "9")
    onExecutorAdded(manager, "10")
    onExecutorAdded(manager, "11")
    onExecutorAdded(manager, "12")
    onExecutorAdded(manager, "13")
    assert(executorIds(manager).size === 10)

    // Remove succeeds again, now that we are no longer at the lower limit
    //再次删除成功,不再在较低的限制
    assert(removeExecutor(manager, "4"))
    assert(removeExecutor(manager, "5"))
    assert(removeExecutor(manager, "6"))
    assert(removeExecutor(manager, "7"))
    assert(executorIds(manager).size === 10)
    assert(addExecutors(manager) === 0)
    onExecutorRemoved(manager, "4")
    onExecutorRemoved(manager, "5")
    assert(executorIds(manager).size === 8)

    // Number of executors pending restarts at 1
    //一些执行者等待重启1
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 0)
    assert(executorIds(manager).size === 8)
    onExecutorRemoved(manager, "6")
    onExecutorRemoved(manager, "7")
    onExecutorAdded(manager, "14")
    onExecutorAdded(manager, "15")
    assert(executorIds(manager).size === 8)
    assert(addExecutors(manager) === 0) // still at upper limit 仍在上限
    onExecutorAdded(manager, "16")
    onExecutorAdded(manager, "17")
    assert(executorIds(manager).size === 10)
    assert(numExecutorsTarget(manager) === 10)
  }

  test("starting/canceling add timer") {//启动/取消添加定时器
    sc = createSparkContext(2, 10, 2)
    val clock = new ManualClock(8888L)
    val manager = sc.executorAllocationManager.get
    manager.setClock(clock)

    // Starting add timer is idempotent
    //开始添加定时器
    assert(addTime(manager) === NOT_SET)
    onSchedulerBacklogged(manager)
    val firstAddTime = addTime(manager)
    assert(firstAddTime === clock.getTimeMillis + schedulerBacklogTimeout * 1000)
    clock.advance(100L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === firstAddTime) // timer is already started
    clock.advance(200L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === firstAddTime)
    onSchedulerQueueEmpty(manager)

    // Restart add timer 重新添加定时器
    clock.advance(1000L)
    assert(addTime(manager) === NOT_SET)
    onSchedulerBacklogged(manager)
    val secondAddTime = addTime(manager)
    assert(secondAddTime === clock.getTimeMillis + schedulerBacklogTimeout * 1000)
    clock.advance(100L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === secondAddTime) // timer is already started 计时器已经启动
    assert(addTime(manager) !== firstAddTime)
    assert(firstAddTime !== secondAddTime)
  }

  test("starting/canceling remove timers") {//启动/取消删除定时器
    sc = createSparkContext(2, 10, 2)
    val clock = new ManualClock(14444L)
    val manager = sc.executorAllocationManager.get
    manager.setClock(clock)

    executorIds(manager).asInstanceOf[mutable.Set[String]] ++= List("1", "2", "3")

    // Starting remove timer is idempotent for each executor
    //开始删除定时器运行的每个执行者
    assert(removeTimes(manager).isEmpty)
    onExecutorIdle(manager, "1")
    assert(removeTimes(manager).size === 1)
    assert(removeTimes(manager).contains("1"))
    val firstRemoveTime = removeTimes(manager)("1")
    assert(firstRemoveTime === clock.getTimeMillis + executorIdleTimeout * 1000)
    clock.advance(100L)
    onExecutorIdle(manager, "1")
    assert(removeTimes(manager)("1") === firstRemoveTime) // timer is already started
    clock.advance(200L)
    onExecutorIdle(manager, "1")
    assert(removeTimes(manager)("1") === firstRemoveTime)
    clock.advance(300L)
    onExecutorIdle(manager, "2")
    assert(removeTimes(manager)("2") !== firstRemoveTime) // different executor
    assert(removeTimes(manager)("2") === clock.getTimeMillis + executorIdleTimeout * 1000)
    clock.advance(400L)
    onExecutorIdle(manager, "3")
    assert(removeTimes(manager)("3") !== firstRemoveTime)
    assert(removeTimes(manager)("3") === clock.getTimeMillis + executorIdleTimeout * 1000)
    assert(removeTimes(manager).size === 3)
    assert(removeTimes(manager).contains("2"))
    assert(removeTimes(manager).contains("3"))

    // Restart remove timer 重启删除定时器
    clock.advance(1000L)
    onExecutorBusy(manager, "1")
    assert(removeTimes(manager).size === 2)
    onExecutorIdle(manager, "1")
    assert(removeTimes(manager).size === 3)
    assert(removeTimes(manager).contains("1"))
    val secondRemoveTime = removeTimes(manager)("1")
    assert(secondRemoveTime === clock.getTimeMillis + executorIdleTimeout * 1000)
    assert(removeTimes(manager)("1") === secondRemoveTime) // timer is already started
    assert(removeTimes(manager)("1") !== firstRemoveTime)
    assert(firstRemoveTime !== secondRemoveTime)
  }

  test("mock polling loop with no events") {//没有事件的模拟循环
    sc = createSparkContext(0, 20, 0)
    val manager = sc.executorAllocationManager.get
    val clock = new ManualClock(2020L)
    manager.setClock(clock)

    // No events - we should not be adding or removing
    //没有事件-我们不应该添加或删除
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(100L)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(1000L)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(10000L)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test("mock polling loop add behavior") {//模拟循环添加行为
    sc = createSparkContext(0, 20, 0)
    val clock = new ManualClock(2020L)
    val manager = sc.executorAllocationManager.get
    manager.setClock(clock)
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Scheduler queue backlogged 调度队列积压
    onSchedulerBacklogged(manager)
    clock.advance(schedulerBacklogTimeout * 1000 / 2)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0) // timer not exceeded yet 定时器不超过
    clock.advance(schedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 1) // first timer exceeded 第一时间超过
    clock.advance(sustainedSchedulerBacklogTimeout * 1000 / 2)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 1) // second timer not exceeded yet 第二个定时器不超过
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 1 + 2) // second timer exceeded 第二计时器超过
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 1 + 2 + 4) // third timer exceeded 第三定时器超过

    // Scheduler queue drained 调度队列排
    onSchedulerQueueEmpty(manager)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7) // timer is canceled 定时器是取消
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7)

    // Scheduler queue backlogged again
    //调度队列积压了
    onSchedulerBacklogged(manager)
    clock.advance(schedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7 + 1) // timer restarted 定时器启动
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7 + 1 + 2)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7 + 1 + 2 + 4)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 20) // limit reached 达到了极限
  }

  test("mock polling loop remove behavior") {//模拟循环删除行为
    sc = createSparkContext(1, 20, 1)
    val clock = new ManualClock(2020L)
    val manager = sc.executorAllocationManager.get
    manager.setClock(clock)

    // Remove idle executors on timeout
    //删除超时执行器
    onExecutorAdded(manager, "executor-1")
    onExecutorAdded(manager, "executor-2")
    onExecutorAdded(manager, "executor-3")
    assert(removeTimes(manager).size === 3)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(executorIdleTimeout * 1000 / 2)
    schedule(manager)
    assert(removeTimes(manager).size === 3) // idle threshold not reached yet 未达到空闲阈值
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(executorIdleTimeout * 1000)
    schedule(manager)
    assert(removeTimes(manager).isEmpty) // idle threshold exceeded 闲置的阈值超过
    assert(executorsPendingToRemove(manager).size === 2) // limit reached (1 executor remaining) 达到了极限

    // Mark a subset as busy - only idle executors should be removed
    //标记一个子集,只能闲时的删除执行者
    onExecutorAdded(manager, "executor-4")
    onExecutorAdded(manager, "executor-5")
    onExecutorAdded(manager, "executor-6")
    onExecutorAdded(manager, "executor-7")
    assert(removeTimes(manager).size === 5)              // 5 active executors 5活动的执行者
    assert(executorsPendingToRemove(manager).size === 2) // 2 pending to be removed 2等待被删除
    onExecutorBusy(manager, "executor-4")
    onExecutorBusy(manager, "executor-5")
    onExecutorBusy(manager, "executor-6") // 3 busy and 2 idle (of the 5 active ones) 3忙和2闲
    schedule(manager)
    assert(removeTimes(manager).size === 2) // remove only idle executors 只删除闲置的执行者
    assert(!removeTimes(manager).contains("executor-4"))
    assert(!removeTimes(manager).contains("executor-5"))
    assert(!removeTimes(manager).contains("executor-6"))
    assert(executorsPendingToRemove(manager).size === 2)
    clock.advance(executorIdleTimeout * 1000)
    schedule(manager)
    assert(removeTimes(manager).isEmpty) // idle executors are removed 删除闲置的执行者
    assert(executorsPendingToRemove(manager).size === 4)
    assert(!executorsPendingToRemove(manager).contains("executor-4"))
    assert(!executorsPendingToRemove(manager).contains("executor-5"))
    assert(!executorsPendingToRemove(manager).contains("executor-6"))

    // Busy executors are now idle and should be removed
    //删除忙碌和闲置执行者
    onExecutorIdle(manager, "executor-4")
    onExecutorIdle(manager, "executor-5")
    onExecutorIdle(manager, "executor-6")
    schedule(manager)
    assert(removeTimes(manager).size === 3) // 0 busy and 3 idle 0忙和3闲
    assert(removeTimes(manager).contains("executor-4"))
    assert(removeTimes(manager).contains("executor-5"))
    assert(removeTimes(manager).contains("executor-6"))
    assert(executorsPendingToRemove(manager).size === 4)
    clock.advance(executorIdleTimeout * 1000)
    schedule(manager)
    assert(removeTimes(manager).isEmpty)
    assert(executorsPendingToRemove(manager).size === 6) // limit reached (1 executor remaining) 达到了极限
  }

  test("listeners trigger add executors correctly") {//监听添加触发执行者
    sc = createSparkContext(2, 10, 2)
    val manager = sc.executorAllocationManager.get
    assert(addTime(manager) === NOT_SET)

    // Starting a stage should start the add timer
    //启动一个阶段应该启动添加定时器
    val numTasks = 10
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, numTasks)))
    assert(addTime(manager) !== NOT_SET)

    // Starting a subset of the tasks should not cancel the add timer
    //启动任务的一个子集不应该取消添加定时器
    val taskInfos = (0 to numTasks - 1).map { i => createTaskInfo(i, i, "executor-1") }
    taskInfos.tail.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, info)) }
    assert(addTime(manager) !== NOT_SET)

    // Starting all remaining tasks should cancel the add timer
    //启动所有剩下的任务应该取消添加计时器
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, taskInfos.head))
    assert(addTime(manager) === NOT_SET)

    // Start two different stages 开始两个不同的阶段
    // The add timer should be canceled only if all tasks in both stages start running
    //只有在两个阶段的所有任务开始运行时,才应该取消添加计时器
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, numTasks)))
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, numTasks)))
    assert(addTime(manager) !== NOT_SET)
    taskInfos.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(1, 0, info)) }
    assert(addTime(manager) !== NOT_SET)
    taskInfos.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, info)) }
    assert(addTime(manager) === NOT_SET)
  }

  test("listeners trigger remove executors correctly") {//删除执行触发监听器
    sc = createSparkContext(2, 10, 2)
    val manager = sc.executorAllocationManager.get
    assert(removeTimes(manager).isEmpty)

    // Added executors should start the remove timers for each executor
    //添加执行器应每个执行者删除定时器
    (1 to 5).map("executor-" + _).foreach { id => onExecutorAdded(manager, id) }
    assert(removeTimes(manager).size === 5)

    // Starting a task cancel the remove timer for that executor
    //启动一个任务取消该执行器的删除计时器
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(1, 1, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(2, 2, "executor-2")))
    assert(removeTimes(manager).size === 3)
    assert(!removeTimes(manager).contains("executor-1"))
    assert(!removeTimes(manager).contains("executor-2"))

    // Finishing all tasks running on an executor should start the remove timer for that executor
    //完成执行器上运行的所有任务,应启动该执行器的删除计时器
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(0, 0, "executor-1"), new TaskMetrics))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(2, 2, "executor-2"), new TaskMetrics))
    assert(removeTimes(manager).size === 4)
    //未完成
    assert(!removeTimes(manager).contains("executor-1")) // executor-1 has not finished yet
    assert(removeTimes(manager).contains("executor-2"))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(1, 1, "executor-1"), new TaskMetrics))
    assert(removeTimes(manager).size === 5)
    //未完成
    assert(removeTimes(manager).contains("executor-1")) // executor-1 has now finished
  }
  //添加和删除executor触发监听器
  test("listeners trigger add and remove executor callbacks correctly") {
    sc = createSparkContext(2, 10, 2)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(removeTimes(manager).isEmpty)

    // New executors have registered
    //注册新executors
    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(removeTimes(manager).size === 1)
    assert(removeTimes(manager).contains("executor-1"))
    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-2", new ExecutorInfo("host2", 1, Map.empty)))
    assert(executorIds(manager).size === 2)
    assert(executorIds(manager).contains("executor-2"))
    assert(removeTimes(manager).size === 2)
    assert(removeTimes(manager).contains("executor-2"))

    // Existing executors have disconnected
    //断开现有的executors连接
    sc.listenerBus.postToAll(SparkListenerExecutorRemoved(0L, "executor-1", ""))
    assert(executorIds(manager).size === 1)
    assert(!executorIds(manager).contains("executor-1"))
    assert(removeTimes(manager).size === 1)
    assert(!removeTimes(manager).contains("executor-1"))

    // Unknown executor has disconnected
    //断开未知的执行者连接
    sc.listenerBus.postToAll(SparkListenerExecutorRemoved(0L, "executor-3", ""))
    assert(executorIds(manager).size === 1)
    assert(removeTimes(manager).size === 1)
  }

  test("SPARK-4951: call onTaskStart before onBlockManagerAdded") {
    sc = createSparkContext(2, 10, 2)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(removeTimes(manager).isEmpty)

    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(removeTimes(manager).size === 0)
  }

  test("SPARK-4951: onExecutorAdded should not add a busy executor to removeTimes") {
    sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(removeTimes(manager).isEmpty)
    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))

    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(removeTimes(manager).size === 0)

    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-2", new ExecutorInfo("host1", 1, Map.empty)))
    assert(executorIds(manager).size === 2)
    assert(executorIds(manager).contains("executor-2"))
    assert(removeTimes(manager).size === 1)
    assert(removeTimes(manager).contains("executor-2"))
    assert(!removeTimes(manager).contains("executor-1"))
  }
  //避免增加当目标大于正在运行executor
  test("avoid ramp up when target < running executors") {
    sc = createSparkContext(0, 100000, 0)
    val manager = sc.executorAllocationManager.get
    val stage1 = createStageInfo(0, 1000)
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(stage1))

    assert(addExecutors(manager) === 1)
    assert(addExecutors(manager) === 2)
    assert(addExecutors(manager) === 4)
    assert(addExecutors(manager) === 8)
    assert(numExecutorsTarget(manager) === 15)
    (0 until 15).foreach { i =>
      onExecutorAdded(manager, s"executor-$i")
    }
    assert(executorIds(manager).size === 15)
    sc.listenerBus.postToAll(SparkListenerStageCompleted(stage1))

    adjustRequestedExecutors(manager)
    assert(numExecutorsTarget(manager) === 0)

    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, 1000)))
    addExecutors(manager)
    assert(numExecutorsTarget(manager) === 16)
  }
  //避免初始化执行者直到提交第一份Job
  test("avoid ramp down initial executors until first job is submitted") {
    sc = createSparkContext(2, 5, 3)
    val manager = sc.executorAllocationManager.get
    val clock = new ManualClock(10000L)
    manager.setClock(clock)

    // Verify the initial number of executors
    //验证执行的初始数量
    assert(numExecutorsTarget(manager) === 3)
    schedule(manager)
    // Verify whether the initial number of executors is kept with no pending tasks
    //验证是否执行初始化数是一直没有挂起的任务
    assert(numExecutorsTarget(manager) === 3)

    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, 2)))
    clock.advance(100L)

    assert(maxNumExecutorsNeeded(manager) === 2)
    schedule(manager)

    // Verify that current number of executors should be ramp down when first job is submitted
    //验证当前一些执行者第一份Job的提交
    assert(numExecutorsTarget(manager) === 2)
  }

  test("avoid ramp down initial executors until idle executor is timeout") {
    sc = createSparkContext(2, 5, 3)
    val manager = sc.executorAllocationManager.get
    val clock = new ManualClock(10000L)
    manager.setClock(clock)

    // Verify the initial number of executors
    //验证执行的初始数量
    assert(numExecutorsTarget(manager) === 3)
    schedule(manager)
    // Verify the initial number of executors is kept when no pending tasks
    //验证执行的初始数量保持在没有挂起的任务
    assert(numExecutorsTarget(manager) === 3)
    (0 until 3).foreach { i =>
      onExecutorAdded(manager, s"executor-$i")
    }

    clock.advance(executorIdleTimeout * 1000)

    assert(maxNumExecutorsNeeded(manager) === 0)
    schedule(manager)
    // Verify executor is timeout but numExecutorsTarget is not recalculated
    //验证执行超时的numexecutorstarget不重新计算
    assert(numExecutorsTarget(manager) === 3)

    // Schedule again to recalculate the numExecutorsTarget after executor is timeout
    //执行超时后调度再重新计算numexecutorstarget
    schedule(manager)
    // Verify that current number of executors should be ramp down when executor is timeout
    //验证当前的一些执行者执行时超时
    assert(numExecutorsTarget(manager) === 2)
  }
  //获取待定的任务数和相关的最佳位置
  test("get pending task number and related locality preference") {
    sc = createSparkContext(2, 5, 3)
    val manager = sc.executorAllocationManager.get

    val localityPreferences1 = Seq(
      Seq(TaskLocation("host1"), TaskLocation("host2"), TaskLocation("host3")),
      Seq(TaskLocation("host1"), TaskLocation("host2"), TaskLocation("host4")),
      Seq(TaskLocation("host2"), TaskLocation("host3"), TaskLocation("host4")),
      Seq.empty,
      Seq.empty
    )
    val stageInfo1 = createStageInfo(1, 5, localityPreferences1)
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(stageInfo1))

    assert(localityAwareTasks(manager) === 3)
    assert(hostToLocalTaskCount(manager) ===
      Map("host1" -> 2, "host2" -> 3, "host3" -> 2, "host4" -> 2))

    val localityPreferences2 = Seq(
      Seq(TaskLocation("host2"), TaskLocation("host3"), TaskLocation("host5")),
      Seq(TaskLocation("host3"), TaskLocation("host4"), TaskLocation("host5")),
      Seq.empty
    )
    val stageInfo2 = createStageInfo(2, 3, localityPreferences2)
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(stageInfo2))

    assert(localityAwareTasks(manager) === 5)
    assert(hostToLocalTaskCount(manager) ===
      Map("host1" -> 2, "host2" -> 4, "host3" -> 4, "host4" -> 3, "host5" -> 2))

    sc.listenerBus.postToAll(SparkListenerStageCompleted(stageInfo1))
    assert(localityAwareTasks(manager) === 2)
    assert(hostToLocalTaskCount(manager) ===
      Map("host2" -> 1, "host3" -> 2, "host4" -> 1, "host5" -> 2))
  }
  //妥善处理失败的任务
  test("SPARK-8366: maxNumExecutorsNeeded should properly handle failed tasks") {
    sc = createSparkContext()
    val manager = sc.executorAllocationManager.get
    assert(maxNumExecutorsNeeded(manager) === 0)

    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1)))
    assert(maxNumExecutorsNeeded(manager) === 1)

    val taskInfo = createTaskInfo(1, 1, "executor-1")
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, taskInfo))
    assert(maxNumExecutorsNeeded(manager) === 1)

    // If the task is failed, we expect it to be resubmitted later.
    //如果任务失败,我们希望它被提交后
    val taskEndReason = ExceptionFailure(null, null, null, null, null, None)
    sc.listenerBus.postToAll(SparkListenerTaskEnd(0, 0, null, taskEndReason, taskInfo, null))
    assert(maxNumExecutorsNeeded(manager) === 1)
  }

  private def createSparkContext(
      minExecutors: Int = 1,
      maxExecutors: Int = 5,
      initialExecutors: Int = 1): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", minExecutors.toString)
      .set("spark.dynamicAllocation.maxExecutors", maxExecutors.toString)
      .set("spark.dynamicAllocation.initialExecutors", initialExecutors.toString)
      .set("spark.dynamicAllocation.schedulerBacklogTimeout",
          s"${schedulerBacklogTimeout.toString}s")
      .set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout",
        s"${sustainedSchedulerBacklogTimeout.toString}s")
      .set("spark.dynamicAllocation.executorIdleTimeout", s"${executorIdleTimeout.toString}s")
      .set("spark.dynamicAllocation.testing", "true")
    val sc = new SparkContext(conf)
    contexts += sc
    sc
  }

}

/**
 * Helper methods for testing ExecutorAllocationManager.
 * This includes methods to access private methods and fields in ExecutorAllocationManager.
  * 这包括访问ExecutorAllocationManager中的私有方法和字段的方法
 */
private object ExecutorAllocationManagerSuite extends PrivateMethodTester {
  private val schedulerBacklogTimeout = 1L
  private val sustainedSchedulerBacklogTimeout = 2L
  private val executorIdleTimeout = 3L

  private def createStageInfo(
      stageId: Int,
      numTasks: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
    ): StageInfo = {
    new StageInfo(
      stageId, 0, "name", numTasks, Seq.empty, Seq.empty, "no details", taskLocalityPreferences)
  }

  private def createTaskInfo(taskId: Int, taskIndex: Int, executorId: String): TaskInfo = {
    new TaskInfo(taskId, taskIndex, 0, 0, executorId, "", TaskLocality.ANY, speculative = false)
  }

  /* ------------------------------------------------------- *
   | Helper methods for accessing private methods and fields |
   | 辅助方法用于访问私有方法和字段                    										 |
   * ------------------------------------------------------- */

  private val _numExecutorsToAdd = PrivateMethod[Int]('numExecutorsToAdd)
  private val _numExecutorsTarget = PrivateMethod[Int]('numExecutorsTarget)
  private val _maxNumExecutorsNeeded = PrivateMethod[Int]('maxNumExecutorsNeeded)
  private val _executorsPendingToRemove =
    PrivateMethod[collection.Set[String]]('executorsPendingToRemove)
  private val _executorIds = PrivateMethod[collection.Set[String]]('executorIds)
  private val _addTime = PrivateMethod[Long]('addTime)
  private val _removeTimes = PrivateMethod[collection.Map[String, Long]]('removeTimes)
  private val _schedule = PrivateMethod[Unit]('schedule)
  private val _addExecutors = PrivateMethod[Int]('addExecutors)
  private val _updateAndSyncNumExecutorsTarget =
    PrivateMethod[Int]('updateAndSyncNumExecutorsTarget)
  private val _removeExecutor = PrivateMethod[Boolean]('removeExecutor)
  private val _onExecutorAdded = PrivateMethod[Unit]('onExecutorAdded)
  private val _onExecutorRemoved = PrivateMethod[Unit]('onExecutorRemoved)
  private val _onSchedulerBacklogged = PrivateMethod[Unit]('onSchedulerBacklogged)
  private val _onSchedulerQueueEmpty = PrivateMethod[Unit]('onSchedulerQueueEmpty)
  private val _onExecutorIdle = PrivateMethod[Unit]('onExecutorIdle)
  private val _onExecutorBusy = PrivateMethod[Unit]('onExecutorBusy)
  private val _localityAwareTasks = PrivateMethod[Int]('localityAwareTasks)
  private val _hostToLocalTaskCount = PrivateMethod[Map[String, Int]]('hostToLocalTaskCount)

  //增加的执行数
  private def numExecutorsToAdd(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _numExecutorsToAdd()
  }
  //在此时刻所需的执行数,如果我们的所有执行者都要死亡,我们立即从集群管理器获得的执行数
  private def numExecutorsTarget(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _numExecutorsTarget()
  }
  //如果已经等待被杀死,不要再次杀死执行者
  private def executorsPendingToRemove(
      manager: ExecutorAllocationManager): collection.Set[String] = {
    manager invokePrivate _executorsPendingToRemove()
  }
  //所有已知executors
  private def executorIds(manager: ExecutorAllocationManager): collection.Set[String] = {
    manager invokePrivate _executorIds()
  }
  //添加时间
  private def addTime(manager: ExecutorAllocationManager): Long = {
    manager invokePrivate _addTime()
  }
  //移除时间
  private def removeTimes(manager: ExecutorAllocationManager): collection.Map[String, Long] = {
    manager invokePrivate _removeTimes()
  }
  //
  private def schedule(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _schedule()
  }
  //最大Executros数
  private def maxNumExecutorsNeeded(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _maxNumExecutorsNeeded()
  }
  //从集群管理器请求的执行者,如果在执行者的数量达到上限,放弃执行数复位而不是继续添加下一轮,
  //还回执行者的实际数量
  private def addExecutors(manager: ExecutorAllocationManager): Int = {
    val maxNumExecutorsNeeded = manager invokePrivate _maxNumExecutorsNeeded()
    manager invokePrivate _addExecutors(maxNumExecutorsNeeded)
  }
//同步更新集群管理器执行目标数
  private def adjustRequestedExecutors(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _updateAndSyncNumExecutorsTarget(0L)
  }
  //请求集群管理器删除给定的执行器,返回是否收到请求。
  private def removeExecutor(manager: ExecutorAllocationManager, id: String): Boolean = {
    manager invokePrivate _removeExecutor(id)
  }
  //调用指定的执行时调用回调函数
  private def onExecutorAdded(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorAdded(id)
  }
  //当指定的执行程序已被删除时调用回调函数
  private def onExecutorRemoved(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorRemoved(id)
  }
  //调度程序接收新的挂起任务时调用回调,这将在未来确定何时添加执行,如果还没有设置
  private def onSchedulerBacklogged(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _onSchedulerBacklogged()
  }
  //调度器队列排出时调用回调函数,这将重置用于添加执行程序的所有变量。
  private def onSchedulerQueueEmpty(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _onSchedulerQueueEmpty()
  }
  //当指定的执行程序不再运行任何任务时调用回调函数
  private def onExecutorIdle(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorIdle(id)
  }
  //当指定的执行程序正在运行任务时调用回调,这将重置用于删除此执行程序的所有变量。
  private def onExecutorBusy(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorBusy(id)
  }
  //更新Executor位置提示(具有本地偏好设置的任务数,每对是一个节点的映射以及该节点上要安排的任务数)。
  private def localityAwareTasks(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _localityAwareTasks()
  }
  //运行主机上的任务
  private def hostToLocalTaskCount(manager: ExecutorAllocationManager): Map[String, Int] = {
    manager invokePrivate _hostToLocalTaskCount()
  }
}
