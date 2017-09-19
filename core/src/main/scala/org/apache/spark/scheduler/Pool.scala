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

import java.util.concurrent.{ ConcurrentHashMap, ConcurrentLinkedQueue }

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
 * 代表一个可调度的实体集合池或tasksetmanagers,
 */

private[spark] class Pool(
  val poolName: String,
  val schedulingMode: SchedulingMode,
  initMinShare: Int,
  initWeight: Int)
    extends Schedulable
    with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  var weight = initWeight
  var minShare = initMinShare
  var runningTasks = 0 //运行Task数
  var priority = 0 //优先级

  // A pool's stage id is used to break the tie in scheduling.
  //池的阶段id用于打破调度的关系
  var stageId = -1
  var name = poolName
  var parent: Pool = null

  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
    }
  }

  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }
  //丢失executor
  override def executorLost(executorId: String, host: String) {
    schedulableQueue.foreach(_.executorLost(executorId, host))
  }
  //推测
  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue) {
      shouldRevive |= schedulable.checkSpeculatableTasks()
    }
    shouldRevive
  }
  //对rootPool中的所有TaskSetManager按照调度算法排序
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    //创建一个ArrayBuffer,存储TaskSetManager  
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    // taskSetSchedulingAlgorithm为调度算法,包括FIFO和FAIR两种  
    // 这里针对调度队列,按照调度算法对其排序,生成一个序列sortedSchedulableQueue  
    val sortedSchedulableQueue =
      schedulableQueue.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    //循环sortedSchedulableQueue中所有的TaskSetManager,通过其getSortedTaskSetQueue来填充sortedTaskSetQueue  
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    //返回sortedTaskSetQueue 
    sortedTaskSetQueue
  }
  //增加运行Task
  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }
  //减少运行Task
  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
