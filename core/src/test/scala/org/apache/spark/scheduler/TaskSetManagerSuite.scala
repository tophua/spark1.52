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

import java.util.Random

import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.ManualClock

class FakeDAGScheduler(sc: SparkContext, taskScheduler: FakeTaskScheduler)
    extends DAGScheduler(sc) {

  override def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    taskScheduler.startedTasks += taskInfo.index
  }

  override def taskEnded(
    task: Task[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Map[Long, Any],
    taskInfo: TaskInfo,
    taskMetrics: TaskMetrics) {
    taskScheduler.endedTasks(taskInfo.index) = reason
  }

  override def executorAdded(execId: String, host: String) {}

  override def executorLost(execId: String) {}

  override def taskSetFailed(
    taskSet: TaskSet,
    reason: String,
    exception: Option[Throwable]): Unit = {
    taskScheduler.taskSetsFailed += taskSet.id
  }
}

// Get the rack for a given host
//给定一个主机获得机架
object FakeRackUtil {
  private val hostToRack = new mutable.HashMap[String, String]()

  def cleanUp() {
    hostToRack.clear()
  }

  def assignHostToRack(host: String, rack: String) {
    hostToRack(host) = rack
  }

  def getRackForHost(host: String): Option[String] = {
    hostToRack.get(host)
  }
}

/**
 * A mock TaskSchedulerImpl implementation that just remembers information about tasks started and
 * feedback received from the TaskSetManagers. Note that it's important to initialize this with
 * 模拟taskschedulerimpl实现,只是记得有关任务开始从tasksetmanagers收到反馈,
 * a list of "live" executors and their hostnames for isExecutorAlive and hasExecutorsAliveOnHost
 * to work, and these are required for locality in TaskSetManager.
 * 注意:这是重要的初始化这一系列的“活”的执行器和他们的isexecutoralive和hasexecutorsaliveonhost工作主机名
 * 这是本地的tasksetmanager请求
 */
class FakeTaskScheduler(sc: SparkContext, liveExecutors: (String, String)* /* execId, host */ )
    extends TaskSchedulerImpl(sc) {
  val startedTasks = new ArrayBuffer[Long]
  val endedTasks = new mutable.HashMap[Long, TaskEndReason]
  val finishedManagers = new ArrayBuffer[TaskSetManager]
  val taskSetsFailed = new ArrayBuffer[String]

  val executors = new mutable.HashMap[String, String]
  for ((execId, host) <- liveExecutors) {
    addExecutor(execId, host)
  }

  for ((execId, host) <- liveExecutors; rack <- getRackForHost(host)) {
    hostsByRack.getOrElseUpdate(rack, new mutable.HashSet[String]()) += host
  }

  dagScheduler = new FakeDAGScheduler(sc, this)

  def removeExecutor(execId: String) {
    executors -= execId
    val host = executorIdToHost.get(execId)
    assert(host != None)
    val hostId = host.get
    val executorsOnHost = executorsByHost(hostId)
    executorsOnHost -= execId
    for (rack <- getRackForHost(hostId); hosts <- hostsByRack.get(rack)) {
      hosts -= hostId
      if (hosts.isEmpty) {
        hostsByRack -= rack
      }
    }
  }

  override def taskSetFinished(manager: TaskSetManager): Unit = finishedManagers += manager

  override def isExecutorAlive(execId: String): Boolean = executors.contains(execId)

  override def hasExecutorsAliveOnHost(host: String): Boolean = executors.values.exists(_ == host)

  override def hasHostAliveOnRack(rack: String): Boolean = {
    hostsByRack.get(rack) != None
  }

  def addExecutor(execId: String, host: String) {
    executors.put(execId, host)
    val executorsOnHost = executorsByHost.getOrElseUpdate(host, new mutable.HashSet[String])
    executorsOnHost += execId
    executorIdToHost += execId -> host
    for (rack <- getRackForHost(host)) {
      hostsByRack.getOrElseUpdate(rack, new mutable.HashSet[String]()) += host
    }
  }

  override def getRackForHost(value: String): Option[String] = FakeRackUtil.getRackForHost(value)
}

/**
 * A Task implementation that results in a large serialized task.
 * 一个任务的执行,结果在一个大的系列任务
 */
class LargeTask(stageId: Int) extends Task[Array[Byte]](stageId, 0, 0, Seq.empty) {
  val randomBuffer = new Array[Byte](TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024)
  val random = new Random(0)
  random.nextBytes(randomBuffer)

  override def runTask(context: TaskContext): Array[Byte] = randomBuffer
  override def preferredLocations: Seq[TaskLocation] = Seq[TaskLocation]()
}

class TaskSetManagerSuite extends SparkFunSuite with LocalSparkContext with Logging {
  import TaskLocality.{ ANY, PROCESS_LOCAL, NO_PREF, NODE_LOCAL, RACK_LOCAL }

  private val conf = new SparkConf

  val LOCALITY_WAIT_MS = conf.getTimeAsMs("spark.locality.wait", "3s")
  val MAX_TASK_FAILURES = 4

  override def beforeEach() {
    super.beforeEach()
    FakeRackUtil.cleanUp()
  }

  test("TaskSet with no preferences") { //taskset没有最佳位置
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // Offer a host with NO_PREF as the constraint,
    //提供一系列no_pref为约束
    // we should get a nopref task immediately since that's what we only have
    //我们应该立即得到nopref任务,因为这是我们唯一的
    var taskOption = manager.resourceOffer("exec1", "host1", NO_PREF)
    assert(taskOption.isDefined)

    // Tell it the task has finished
    // 告诉任务已经完成了
    manager.handleSuccessfulTask(0, createTaskResult(0))
    assert(sched.endedTasks(0) === Success)
    assert(sched.finishedManagers.contains(manager))
  }

  test("multiple offers with no preferences") { //没有提供多个最佳位置
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(3)
    //在taskSchedulerImpl中,一组任务交给一个新的TaskSetManager实例进行管理,
    //所有的TaskSetManager经由SchedulableBuilder根据特定的调度策略进度排序
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    // First three offers should all find tasks
    //前三个提供的任务应该都能找到
    for (i <- 0 until 3) {
      var taskOption = manager.resourceOffer("exec1", "host1", NO_PREF)
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === "exec1")
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2))

    // Re-offer the host -- now we should get no more tasks
    //重新提供主机-现在我们不应该得到更多的任务
    assert(manager.resourceOffer("exec1", "host1", NO_PREF) === None)

    // Finish the first two tasks
    //完成前两个任务
    manager.handleSuccessfulTask(0, createTaskResult(0))
    manager.handleSuccessfulTask(1, createTaskResult(1))
    assert(sched.endedTasks(0) === Success)
    assert(sched.endedTasks(1) === Success)
    assert(!sched.finishedManagers.contains(manager))

    // Finish the last task
    //完成最后一项任务
    manager.handleSuccessfulTask(2, createTaskResult(2))
    assert(sched.endedTasks(2) === Success)
    assert(sched.finishedManagers.contains(manager))
  }

  test("skip unsatisfiable locality levels") { //跳过不可满足的本地级别
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execC", "host2"))
    val taskSet = FakeTask.createTaskSet(1, Seq(TaskLocation("host1", "execB")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // An executor that is not NODE_LOCAL should be rejected.
    //一个执行人,不应该拒绝node_local
    assert(manager.resourceOffer("execC", "host2", ANY) === None)

    // Because there are no alive PROCESS_LOCAL executors, the base locality level should be
    // NODE_LOCAL. So, we should schedule the task on this offered NODE_LOCAL executor before
    // any of the locality wait timers expire.
    //因为没有活着的PROCESS_LOCAL执行器，所以base locality级别应该是NODE_LOCAL
    //所以，我们应该在任何地点等待计时器到期之前安排这个提供的NODE_LOCAL执行器的任务
    assert(manager.resourceOffer("execA", "host1", ANY).get.index === 0)
  }

  test("basic delay scheduling") { //基本延迟调度
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host2", "exec2")),
      Seq(TaskLocation("host1"), TaskLocation("host2", "exec2")),
      //最后一个任务没有位置参数
      Seq() // Last task has no locality prefs
      )
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    // First offer host1, exec1: first task should be chosen
    //提供第一个主机,exec1:应选择第一个任务
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL) == None)

    clock.advance(LOCALITY_WAIT_MS)
    // Offer host1, exec1 again, at NODE_LOCAL level: the node local (task 2) should
    // get chosen before the noPref task
    //提供主机1,exec1再次,本地节点级别,节点局部(任务2)应该选择在nopref任务
    assert(manager.resourceOffer("exec1", "host1", NODE_LOCAL).get.index == 2)

    // Offer host2, exec3 again, at NODE_LOCAL level: we should choose task 2
    //提供主机2,再一次exec3,
    assert(manager.resourceOffer("exec2", "host2", NODE_LOCAL).get.index == 1)

    // Offer host2, exec3 again, at NODE_LOCAL level: we should get noPref task
    // after failing to find a node_Local task
    //再次提供host2，exec3，在NODE_LOCAL级别：在找不到node_Local任务后，我们应该得到noPref任务
    assert(manager.resourceOffer("exec2", "host2", NODE_LOCAL) == None)
    clock.advance(LOCALITY_WAIT_MS)
    assert(manager.resourceOffer("exec2", "host2", NO_PREF).get.index == 3)
  }
  //我们只有nopref任务队列中,我们不需要延迟调度
  test("we do not need to delay scheduling when we only have noPref tasks in the queue") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec3", "host2"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host2", "exec3")),
      //最后一个任务没有地方设置
      Seq() // Last task has no locality prefs
      )
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    // First offer host1, exec1: first task should be chosen 应选择第一个任务
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL).get.index === 0)
    assert(manager.resourceOffer("exec3", "host2", PROCESS_LOCAL).get.index === 1)
    assert(manager.resourceOffer("exec3", "host2", NODE_LOCAL) == None)
    assert(manager.resourceOffer("exec3", "host2", NO_PREF).get.index === 2)
  }

  test("delay scheduling with fallback") { //延迟调度与回退
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc,
      ("exec1", "host1"), ("exec2", "host2"), ("exec3", "host3"))
    val taskSet = FakeTask.createTaskSet(5,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3")),
      Seq(TaskLocation("host2")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1: first task should be chosen 应选择第一个任务
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // Offer host1 again: nothing should get chosen 什么都不应该被选择
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    clock.advance(LOCALITY_WAIT_MS)

    // Offer host1 again: second task (on host2) should get chosen 第二任务(在第二个主机)应该选择
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 1)

    // Offer host1 again: third task (on host2) should get chosen  第三任务(在第二个主机)应该选择
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 2)

    // Offer host2: fifth task (also on host2) should get chosen
    assert(manager.resourceOffer("exec2", "host2", ANY).get.index === 4)

    // Now that we've launched a local task, we should no longer launch the task for host3
    //现在我们启动一个本地任务,我们不应该在主机3再启动任务
    assert(manager.resourceOffer("exec2", "host2", ANY) === None)

    clock.advance(LOCALITY_WAIT_MS)

    // After another delay, we can go ahead and launch that task non-locally
    //在另一个延迟后,我们可以继续前进,并启动非本地任务
    assert(manager.resourceOffer("exec2", "host2", ANY).get.index === 3)
  }

  test("delay scheduling with failed hosts") { //延迟调度失败的主机
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"),
      ("exec3", "host3"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1: first task should be chosen
    //提供一个主机1,应选择第一个任务
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // After this, nothing should get chosen, because we have separated tasks with unavailable
    // preference from the noPrefPendingTasks
    //在这之后,没有什么应该选择,因为我们无法选择从noprefpendingtasks分离任务
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    // Now mark host2 as dead
    //现在标识主体2已死
    sched.removeExecutor("exec2")
    manager.executorLost("exec2", "host2")

    // nothing should be chosen
    // 什么都不应该选择
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    clock.advance(LOCALITY_WAIT_MS * 2)

    // task 1 and 2 would be scheduled as nonLocal task
    //任务1和2将被定为非本地任务
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 1)
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 2)

    // all finished
    //所有都完成
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)
    assert(manager.resourceOffer("exec2", "host2", ANY) === None)
  }

  test("task result lost") { //任务结果丢失
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // Tell it the task has finished but the result was lost.
    //告诉它任务已经完成,但结果丢失
    manager.handleFailedTask(0, TaskState.FINISHED, TaskResultLost)
    assert(sched.endedTasks(0) === TaskResultLost)

    // Re-offer the host -- now we should get task 0 again.
    //重新提供主机-现在我们应该得到0的任务。
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)
  }
  //重复的失败导致任务集终止
  test("repeated failures lead to task set abortion") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // Fail the task MAX_TASK_FAILURES times, and check that the task set is aborted
    // after the last failure.
    //任务失败max_task_failures数,检查任务集在最后一次失败后终止
    (1 to manager.maxTaskFailures).foreach { index =>
      val offerResult = manager.resourceOffer("exec1", "host1", ANY)
      assert(offerResult.isDefined,
        "Expect resource offer on iteration %s to return a task".format(index))
      assert(offerResult.get.index === 0)
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      if (index < MAX_TASK_FAILURES) {
        assert(!sched.taskSetsFailed.contains(taskSet.id))
      } else {
        assert(sched.taskSetsFailed.contains(taskSet.id))
      }
    }
  }
  //executors将任务失败的列入黑名单后,最佳的优先位置
  test("executors should be blacklisted after task failure, in spite of locality preferences") {
    val rescheduleDelay = 300L
    val conf = new SparkConf().
      set("spark.scheduler.executorTaskBlacklistTime", rescheduleDelay.toString).
      // dont wait to jump locality levels in this test
      //测试不要等待跳转本地性
      set("spark.locality.wait", "0") //本参数是以毫秒为单位启动本地数据task的等待时间

    sc = new SparkContext("local", "test", conf)
    // two executors on same host, one on different.
    //在同一台主机上不同的两个执行者
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"),
      ("exec1.1", "host1"), ("exec2", "host2"))
    // affinity to exec1 on host1 - which we will fail.
    //关联性 exec1 on host1,我们会失败
    val taskSet = FakeTask.createTaskSet(1, Seq(TaskLocation("host1", "exec1")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, 4, clock)

    {
      val offerResult = manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1")

      // Cause exec1 to fail : failure 1
      //exec1失败原因:故障1
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec1 fails after failure 1 due to blacklist
      //确保调度exec1 失败1到黑名单
      assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", NODE_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", RACK_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", ANY).isEmpty)
    }

    // Run the task on exec1.1 - should work, and then fail it on exec1.1
    //运行在exec1.1的任务工作,然后失败了exec1.1
    {
      val offerResult = manager.resourceOffer("exec1.1", "host1", NODE_LOCAL)
      assert(offerResult.isDefined,
        "Expect resource offer to return a task for exec1.1, offerResult = " + offerResult)

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1.1")

      // Cause exec1.1 to fail : failure 2
      //因为exec1.1失败：失败2
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec1.1 fails after failure 2 due to blacklist
      //确保调度exec1.1失效,失败2次后加入“黑名单”
      assert(manager.resourceOffer("exec1.1", "host1", NODE_LOCAL).isEmpty)
    }

    // Run the task on exec2 - should work, and then fail it on exec2
    //运行在exec2的任务工作,然后失败了exec2
    {
      val offerResult = manager.resourceOffer("exec2", "host2", ANY)
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec2")

      // Cause exec2 to fail : failure 3
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec2 fails after failure 3 due to blacklist
      assert(manager.resourceOffer("exec2", "host2", ANY).isEmpty)
    }

    // After reschedule delay, scheduling on exec1 should be possible.
    //延迟后重新调度,调度exec1应该是可能的
    clock.advance(rescheduleDelay)

    {
      val offerResult = manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1")

      assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL).isEmpty)

      // Cause exec1 to fail : failure 4
      //因为exec1失败：失败4
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
    }

    // we have failed the same task 4 times now : task id should now be in taskSetsFailed
    //我们已经失败了4次相同的任务了,现在应该在tasksetsfailed任务ID
    assert(sched.taskSetsFailed.contains(taskSet.id))
  }

  test("new executors get added and lost") { //新执行得到添加和丢失
    // Assign host2 to rack2 分配到主机2到机架2
    FakeRackUtil.assignHostToRack("host2", "rack2")
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc)
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host1", "execB")),
      Seq(TaskLocation("host2", "execC")),
      Seq())
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    // Only ANY is valid
    //只有任何是有效的
    assert(manager.myLocalityLevels.sameElements(Array(NO_PREF, ANY)))
    // Add a new executor
    //添加一个新的执行者
    sched.addExecutor("execD", "host1")
    manager.executorAdded()
    // Valid locality should contain NODE_LOCAL and ANY
    //有效的本地应包含node_local和任何
    assert(manager.myLocalityLevels.sameElements(Array(NODE_LOCAL, NO_PREF, ANY)))
    // Add another executor
    //添加另一个执行者
    sched.addExecutor("execC", "host2")
    manager.executorAdded()
    // Valid locality should contain PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL and ANY
    //有效的地方应包含process_local,node_local,rack_local和任何
    assert(manager.myLocalityLevels.sameElements(
      Array(PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY)))
    // test if the valid locality is recomputed when the executor is lost
    //测试如果本地则重新计算执行丢失
    sched.removeExecutor("execC")
    manager.executorLost("execC", "host2")
    assert(manager.myLocalityLevels.sameElements(Array(NODE_LOCAL, NO_PREF, ANY)))
    sched.removeExecutor("execD")
    manager.executorLost("execD", "host1")
    assert(manager.myLocalityLevels.sameElements(Array(NO_PREF, ANY)))
  }

  test("test RACK_LOCAL tasks") { //测试任务本地机架
    // Assign host1 to rack1
    //分配到主机1到机架1
    FakeRackUtil.assignHostToRack("host1", "rack1")
    // Assign host2 to rack1
    // 分配到主机2到机架1
    FakeRackUtil.assignHostToRack("host2", "rack1")
    // Assign host3 to rack2
    //分配到主机3到机架2
    FakeRackUtil.assignHostToRack("host3", "rack2")
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc,
      ("execA", "host1"), ("execB", "host2"), ("execC", "host3"))
    val taskSet = FakeTask.createTaskSet(2,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host1", "execA")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY)))
    // Set allowed locality to ANY
    //设置允许的本地和任务
    clock.advance(LOCALITY_WAIT_MS * 3)
    // Offer host3 提供主机3
    // No task is scheduled if we restrict locality to RACK_LOCAL
    assert(manager.resourceOffer("execC", "host3", RACK_LOCAL) === None)
    // Task 0 can be scheduled with ANY 任务0可以与任何计划
    assert(manager.resourceOffer("execC", "host3", ANY).get.index === 0)
    // Offer host2
    // Task 1 can be scheduled with RACK_LOCAL
    assert(manager.resourceOffer("execB", "host2", RACK_LOCAL).get.index === 1)
  }

  test("do not emit warning when serialized task is small") { //序列化小任务不发出警告
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    assert(!manager.emittedTaskSizeWarning)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    assert(!manager.emittedTaskSizeWarning)
  }

  test("emit warning when serialized task is large") { //序列化任务大时发出警告
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))

    val taskSet = new TaskSet(Array(new LargeTask(0)), 0, 0, 0, null)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    assert(!manager.emittedTaskSizeWarning)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    assert(manager.emittedTaskSizeWarning)
  }
  //如果任务不能序列化,抛出无法序列化异常
  test("Not serializable exception thrown if the task cannot be serialized") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))

    val taskSet = new TaskSet(
      Array(new NotSerializableFakeTask(1, 0), new NotSerializableFakeTask(0, 1)), 0, 0, 0, null)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    intercept[TaskNotSerializableException] {
      manager.resourceOffer("exec1", "host1", ANY)
    }
    assert(manager.isZombie)
  }

  test("abort the job if total size of results is too large") { //如果结果的总太大,中止作业
    val conf = new SparkConf().set("spark.driver.maxResultSize", "2m")
    sc = new SparkContext("local", "test", conf)

    def genBytes(size: Int): (Int) => Array[Byte] = { (x: Int) =>
      val bytes = Array.ofDim[Byte](size)
      scala.util.Random.nextBytes(bytes)
      bytes
    }

    // multiple 1k result
    //多1k结果
    val r = sc.makeRDD(0 until 10, 10).map(genBytes(1024)).collect()
    assert(10 === r.size)

    // single 10M result
    //单10M结果
    val thrown = intercept[SparkException] { sc.makeRDD(genBytes(10 << 20)(0), 1).collect() }
    assert(thrown.getMessage().contains("bigger than spark.driver.maxResultSize"))

    // multiple 1M results
    //多1M结果
    val thrown2 = intercept[SparkException] {
      sc.makeRDD(0 until 10, 10).map(genBytes(1 << 20)).collect()
    }
    assert(thrown2.getMessage().contains("bigger than spark.driver.maxResultSize"))
  }
  //推理nopref任务应在本地节点调度
  test("speculative and noPref task should be scheduled after node-local") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(
      sc, ("execA", "host1"), ("execB", "host2"), ("execC", "host3"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host2"), TaskLocation("host1")),
      Seq(),
      Seq(TaskLocation("host3", "execC")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL).get.index === 0)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL) == None)
    assert(manager.resourceOffer("execA", "host1", NO_PREF).get.index == 1)

    manager.speculatableTasks += 1
    clock.advance(LOCALITY_WAIT_MS)
    // schedule the nonPref task
    //nonpref任务调度
    assert(manager.resourceOffer("execA", "host1", NO_PREF).get.index === 2)
    // schedule the speculative task
    //调度推理任务
    assert(manager.resourceOffer("execB", "host2", NO_PREF).get.index === 1)
    clock.advance(LOCALITY_WAIT_MS * 3)
    // schedule non-local tasks
    //安排非本地任务
    assert(manager.resourceOffer("execB", "host2", ANY).get.index === 3)
  }
  //节点本地任务应立即安排,当只有节点局部和无偏好任务时
  test("node-local tasks should be scheduled right away " +
    "when there are only node-local and no-preference tasks") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(
      sc, ("execA", "host1"), ("execB", "host2"), ("execC", "host3"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(),
      Seq(TaskLocation("host3")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // node-local tasks are scheduled without delay
    //节点本地任务无延迟
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL).get.index === 0)
    assert(manager.resourceOffer("execA", "host2", NODE_LOCAL).get.index === 1)
    assert(manager.resourceOffer("execA", "host3", NODE_LOCAL).get.index === 3)
    assert(manager.resourceOffer("execA", "host3", NODE_LOCAL) === None)

    // schedule no-preference after node local ones
    // 调度没有最佳位置本地节点
    assert(manager.resourceOffer("execA", "host3", NO_PREF).get.index === 2)
  }
  //本地任务节点,进程本地任务完成后,
  test("SPARK-4939: node-local tasks should be scheduled right after process-local tasks finished") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execB", "host2"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(ExecutorCacheTaskLocation("host1", "execA")),
      Seq(ExecutorCacheTaskLocation("host2", "execB")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // process-local tasks are scheduled first
    //进程本地任务首先被调度
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL).get.index === 2)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL).get.index === 3)
    // node-local tasks are scheduled without delay
    //节点本地任务无延迟
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL).get.index === 0)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL).get.index === 1)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL) == None)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL) == None)
  }
  //没有优先任务应该调度本地进程完成任务
  test("SPARK-4939: no-pref tasks should be scheduled after process-local tasks finished") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execB", "host2"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(),
      Seq(ExecutorCacheTaskLocation("host1", "execA")),
      Seq(ExecutorCacheTaskLocation("host2", "execB")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // process-local tasks are scheduled first
    //进程本地任务首先被调度
    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL).get.index === 1)
    assert(manager.resourceOffer("execB", "host2", PROCESS_LOCAL).get.index === 2)
    // no-pref tasks are scheduled without delay
    //没有优先任务无延迟调度
    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL) == None)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL) == None)
    assert(manager.resourceOffer("execA", "host1", NO_PREF).get.index === 0)
    assert(manager.resourceOffer("execA", "host1", ANY) == None)
  }
  //确保TaskSetManager在添加级别后可用
  test("Ensure TaskSetManager is usable after addition of levels") {
    // Regression test for SPARK-2931
    // 回归测试
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc)
    val taskSet = FakeTask.createTaskSet(2,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host2", "execB.1")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    // Only ANY is valid
    //只有任何是有效的
    assert(manager.myLocalityLevels.sameElements(Array(ANY)))
    // Add a new executor 添加一个新的执行者
    sched.addExecutor("execA", "host1")
    sched.addExecutor("execB.2", "host2")
    manager.executorAdded()
    assert(manager.pendingTasksWithNoPrefs.size === 0)
    // Valid locality should contain PROCESS_LOCAL, NODE_LOCAL and ANY 有效的地方应包含
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    assert(manager.resourceOffer("execA", "host1", ANY) !== None)
    clock.advance(LOCALITY_WAIT_MS * 4)
    assert(manager.resourceOffer("execB.2", "host2", ANY) !== None)
    sched.removeExecutor("execA")
    sched.removeExecutor("execB.2")
    manager.executorLost("execA", "host1")
    manager.executorLost("execB.2", "host2")
    clock.advance(LOCALITY_WAIT_MS * 4)
    sched.addExecutor("execC", "host3")
    manager.executorAdded()
    // Prior to the fix, this line resulted in an ArrayIndexOutOfBoundsException:
    //为解决之前,这导致了一个对象的大小时抛出
    assert(manager.resourceOffer("execC", "host3", ANY) !== None)
  }
 //测试使用HDFSCacheTaskLocation的位置被视为PROCESS_LOCAL
  test("Test that locations with HDFSCacheTaskLocation are treated as PROCESS_LOCAL.") {
    // Regression test for SPARK-2931
    //回归测试
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc,
      ("execA", "host1"), ("execB", "host2"), ("execC", "host3"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(HostTaskLocation("host1")),
      Seq(HostTaskLocation("host2")),
      Seq(HDFSCacheTaskLocation("host3")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    sched.removeExecutor("execA")
    manager.executorAdded()
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    sched.removeExecutor("execB")
    manager.executorAdded()
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    sched.removeExecutor("execC")
    manager.executorAdded()
    assert(manager.myLocalityLevels.sameElements(Array(ANY)))
  }

  def createTaskResult(id: Int): DirectTaskResult[Int] = {
    val valueSer = SparkEnv.get.serializer.newInstance()
    new DirectTaskResult[Int](valueSer.serialize(id), mutable.Map.empty, new TaskMetrics)
  }
}
